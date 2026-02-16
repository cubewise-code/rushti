# Self-Optimization

RushTI learns from each run. It tracks how long each TI process takes and reorders tasks for better parallelization using your chosen scheduling algorithm.

---

## How It Works

1. **Run your workflow** — RushTI records how long each task took in a local SQLite database.
2. **Build a performance picture** — After several runs, RushTI calculates a weighted average duration for each task (recent runs count more).
3. **Opt in to optimization** — Choose a scheduling algorithm via `--optimize` on the CLI or `optimization_algorithm` in a JSON task file. RushTI reorders ready tasks using EWMA estimates.

The result: better worker utilization and shorter total execution time — without changing your task files.

---

## Scheduling Algorithms

RushTI supports two scheduling algorithms. The right choice depends on your workload:

| Algorithm | Sort Order | Best For |
|-----------|-----------|----------|
| `longest_first` | Longest tasks start first | Independent workloads with varied task durations. Starts expensive tasks early so short tasks fill gaps. |
| `shortest_first` | Shortest tasks start first | Shared-resource TM1 workloads where concurrent heavy tasks cause contention (memory, threads, write locks). |

### When to Use Which Algorithm

- **`longest_first`** — Classic bin-packing heuristic. Works well when tasks are independent and the TM1 server has headroom. Ideal for read-heavy extract workflows targeting multiple instances.
- **`shortest_first`** — Reduces peak resource pressure by completing quick tasks first, lowering the number of concurrent heavy tasks. Ideal for write-heavy loads against a single TM1 instance.

!!! tip "Not sure? Start with `shortest_first`"
    Most TM1 workloads involve shared server resources. `shortest_first` is a safer default for typical Planning Analytics environments.

---

## Enable Optimization

Optimization is **off by default**. You opt in explicitly per-run or per-taskfile.

### Via CLI

```bash
# Shortest-first scheduling (good for shared-resource TM1 workloads)
rushti run --tasks daily-refresh.json --max-workers 20 --optimize shortest_first

# Longest-first scheduling (good for independent tasks with varied durations)
rushti run --tasks daily-refresh.json --max-workers 20 --optimize longest_first
```

### Via JSON Task File

Set a default algorithm in the task file's settings section:

```json
{
  "settings": {
    "optimization_algorithm": "shortest_first"
  }
}
```

CLI `--optimize` overrides the JSON setting. Omitting both means no optimization.

### EWMA Tuning (settings.ini)

The EWMA parameters that control how runtime estimates are calculated live in `settings.ini`. These are system-wide and rarely changed:

```ini
[stats]
enabled = true
retention_days = 90

[optimization]
lookback_runs = 10
min_samples = 3
```

!!! info "What's happening under the hood"
    When multiple tasks are ready to execute (i.e., all their predecessors have completed), RushTI sorts them by estimated runtime using the chosen algorithm before assigning them to workers. Dependencies are **never** changed; only the order among independent, ready tasks is affected.

### Requirements

- **`[stats] enabled = true`** — Optimization needs historical data. Without stats, there's nothing to learn from.
- **At least `min_samples` runs** — Tasks with fewer runs than `min_samples` use a default estimate and are placed after tasks with known durations.

---

## Before and After

Consider a simple workflow with six independent tasks and two workers:

**Before optimization (random order):**

```
Worker 1: [Short 2s] [Short 3s] [Long 30s] ──────── Total: 35s
Worker 2: [Short 1s] [Long 25s] ───── [Short 4s]    Total: 30s
                                              Overall: 35s
```

Worker 1 finishes early tasks quickly, then hits the long task last. Meanwhile Worker 2 sits idle waiting.

**After optimization (longest first):**

```
Worker 1: [Long 30s] ──────────── [Short 2s] [Short 1s]  Total: 33s
Worker 2: [Long 25s] ───── [Short 4s] [Short 3s]         Total: 32s
                                                   Overall: 33s
```

Both workers stay busy throughout. The total time drops because long tasks start immediately and short tasks fill in the remaining gaps.

!!! tip "Real-World Gains"
    The improvement depends on how unbalanced your tasks are. Workflows with a mix of 5-second and 5-minute tasks see the biggest gains. Workflows where all tasks take roughly the same time see less benefit.

---

## Configuration

| Setting | Location | Default | Description |
|---------|----------|---------|-------------|
| `--optimize` | CLI | *(none)* | Scheduling algorithm: `longest_first` or `shortest_first`. No flag = no optimization. |
| `optimization_algorithm` | JSON taskfile | *(none)* | Per-taskfile default algorithm. Overridden by CLI `--optimize`. |
| `lookback_runs` | settings.ini | `10` | Number of recent runs to analyze for EWMA estimates |
| `min_samples` | settings.ini | `3` | Minimum successful runs before optimization kicks in |
| `cache_duration_hours` | settings.ini | `24` | How long to cache duration estimates between runs |
| `time_of_day_weighting` | settings.ini | `false` | Weight runs at similar times of day higher (disables caching) |

### How Duration Estimates Work (EWMA)

RushTI uses EWMA (Exponentially Weighted Moving Average) to estimate task durations:

- **Recent runs matter more.** If a process used to take 10 seconds but now takes 20 seconds (because data volumes grew), the estimate adjusts toward 20 seconds.
- **Old outliers fade away.** A one-time spike from a server hiccup does not permanently distort the estimate.
- **The smoothing factor (alpha) is 0.3.** Each new run contributes 30% to the estimate, and the accumulated history contributes 70%.

For most TM1 environments, the default alpha works well. If your process durations change rapidly (for example, during a data migration), see [Performance Tuning](../advanced/performance-tuning.md) for EWMA tuning details.

### Time-of-Day Weighting

Some TI processes run faster at night when the server is idle and slower during business hours. Enable `time_of_day_weighting` to give more weight to runs that happened at a similar time of day:

```ini
[optimization]
time_of_day_weighting = true
```

!!! note
    When `time_of_day_weighting` is enabled, caching is disabled — RushTI recalculates estimates fresh each run to account for the current time of day.

### Choosing `min_samples`

| Workflow Type | Recommended `min_samples` | Reason |
|--------------|--------------------------|--------|
| Stable daily ETL | `3` | Durations are consistent |
| Variable workloads | `5-10` | Need more data points to smooth out variation |
| Seasonal processes | `10-20` | Data volumes change significantly |

---

## Manual Analysis (Optional)

In addition to automatic optimization, RushTI includes a manual analysis tool for generating reports and optimized task files:

```bash
rushti stats analyze \
  --workflow daily-refresh \
  --tasks daily-refresh.json \
  --output daily-refresh-optimized.json
```

This reads your execution history, calculates EWMA estimates, and writes a new task file with tasks reordered for optimal execution. This is useful for:

- **Reviewing performance** — see which tasks are getting slower or faster
- **Sharing an optimized file** — distribute a pre-optimized task file to a team that hasn't enabled automatic optimization
- **Auditing** — compare the original and optimized order to understand what changed

### What the Report Shows

| Field | Description |
|-------|-------------|
| `task_id` | The task identifier from your task file |
| `avg_duration` | Simple average across all analyzed runs |
| `ewma_duration` | Weighted average (recent runs matter more) |
| `min_duration` | Fastest observed execution |
| `max_duration` | Slowest observed execution |
| `run_count` | Number of runs analyzed |

**Reading the results:**

- **EWMA < Average** — The task is getting faster over time
- **EWMA > Average** — The task is getting slower (investigate data volumes or TI process performance)
- **EWMA ≈ Average** — Stable performance

---

## TM1 Integration

If you have [TM1 Integration](tm1-integration.md) enabled (`push_results = true`), optimization data is also visible in TM1. The `rushti` cube stores task durations for every run, so you can build Planning Analytics dashboards that show:

- Which tasks are getting slower over time
- Which workflows benefit most from optimization
- Historical duration trends per TI process
- Success rates and failure patterns

---

## Frequently Asked Questions

### Does optimization change my task dependencies?

No. Optimization only reorders tasks that are independent of each other (tasks at the same depth in the DAG with no shared predecessors). Your dependency chains are never modified.

### How many runs do I need before optimization helps?

At least 3 runs (the `min_samples` default). With fewer runs, the duration estimates are unreliable and RushTI skips optimization for those tasks. For best results, aim for 5-10 runs.

### Which algorithm should I use?

- **`shortest_first`** — Best for most TM1 workloads, especially write-heavy processes against a single TM1 instance. Reduces peak resource contention.
- **`longest_first`** — Best for independent, read-heavy workloads spread across multiple TM1 instances. Classic bin-packing heuristic.

When in doubt, start with `shortest_first`.

### Can I set a default algorithm per task file?

Yes. Add `"optimization_algorithm"` to the JSON task file's settings section. The CLI `--optimize` flag overrides this value at runtime:

```json
{
  "settings": {
    "optimization_algorithm": "shortest_first"
  }
}
```

### Can I optimize a TXT task file?

Yes. Optimization works with both JSON and TXT task files. Use `--optimize <algorithm>` on the CLI. The `stats analyze` command also works with both formats.

### What if I add new tasks to the workflow?

New tasks will not have historical duration data, so RushTI treats them as unknown and places them after tasks with known durations. After a few runs, the new tasks accumulate enough data to be included in optimization.

### Can I use optimization with exclusive mode?

Yes. Optimization and exclusive mode are independent features:

```bash
rushti run --tasks daily-refresh.json --max-workers 4 --exclusive --optimize shortest_first
```

---

## Best Practices

1. **Enable stats from the start.** Even if you do not plan to optimize immediately, having historical data ready is valuable. It costs almost nothing in overhead.

2. **Let it learn.** After enabling optimization, the first few runs build the performance picture. Gains become visible after `min_samples` runs.

3. **Combine with manual tuning.** Optimization reorders tasks but does not make individual TI processes faster. If the stats show a process taking 5 minutes, consider optimizing the TI process itself (better MDX, fewer loops, targeted data clears).

4. **Use `time_of_day_weighting` for mixed schedules.** If the same workflow runs both during business hours and overnight, enable time-of-day weighting to account for load differences.

---

## Customize Further

- **[Performance Tuning](../advanced/performance-tuning.md)** — EWMA alpha tuning, worker sizing, and advanced optimization strategies
- **[Statistics & Dashboards](statistics.md)** — The stats database that powers optimization
- **[Settings Reference](../advanced/settings-reference.md)** — Complete `[optimization]` and `[stats]` settings
