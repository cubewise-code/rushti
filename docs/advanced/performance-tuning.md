# Performance Tuning

This guide covers practical techniques for getting the most out of RushTI's parallel execution engine. The goal is to minimize total workflow runtime by keeping TM1 servers busy without overloading them.

---

## Choosing Worker Count

The `max_workers` setting controls how many TI processes RushTI executes concurrently. The right value depends on your TM1 server capacity and workload characteristics.

### Rules of Thumb

| Scenario | Workers | Reasoning |
|----------|---------|-----------|
| Single TM1 instance, I/O-bound | 4--8 | Avoid overloading one server |
| Single TM1 instance, CPU-bound | 4--6 | CPU contention degrades all tasks |
| Multiple TM1 instances | 8--20 | Load distributes across servers |
| Read-only processes (exports) | 16--32 | Reads rarely contend |
| Write-heavy processes (loads) | 4--8 | Write locks serialize execution anyway |
| Mixed read/write workload | 8--12 | Balanced approach |

### Finding Your Optimal Value

1. Start conservative at `--max-workers 4`.
2. Run the same workflow several times, increasing workers: 8, 12, 16.
3. Watch for diminishing returns -- when doubling workers no longer cuts runtime significantly, you have hit a bottleneck (usually TM1 server threads or write locks).
4. Use `rushti stats visualize` to inspect Gantt charts and identify idle workers.
5. Use `rushti stats optimize` to get a data-driven recommendation based on runs at different worker levels.

```bash
# Collect timing data at different worker levels
rushti run --tasks workflow.json --max-workers 4
rushti run --tasks workflow.json --max-workers 8
rushti run --tasks workflow.json --max-workers 16

# Compare in dashboard
rushti stats visualize --workflow workflow

# Get a data-driven worker recommendation
rushti stats optimize --workflow workflow
```

!!! warning "Too Many Workers"
    Setting workers higher than your TM1 server can handle causes connection pool exhaustion, increased memory usage, and TM1 thread starvation. Symptoms include tasks waiting long periods before starting, sporadic connection timeouts, and TM1 server performance degradation for all users.

---

## Connection Pooling

RushTI reuses TM1py connections across tasks. For each unique TM1 instance referenced in the task file, RushTI creates a pool of connections at startup and shares them among workers.

### Key Behaviors

- **One pool per instance**: If your tasks reference `tm1-finance` and `tm1-reporting`, two independent connection pools are created.
- **Pool size matches workers**: Each pool has up to `max_workers` connections.
- **Connections are reused**: When a worker finishes a task, its connection returns to the pool for the next task targeting the same instance.
- **Session context**: Each connection carries a session context string (`RushTI_{workflow}` or `RushTIX_{workflow}`) for exclusive mode detection.

### Optimizing Connections

In `config.ini`, enable SSL and async mode for better throughput:

```ini
[tm1-finance]
address = tm1server.company.com
port = 12354
user = admin
password = ${TM1_PASSWORD}
ssl = True
async_requests_mode = True
```

### Async Polling (Backoff Strategy)

When `async_requests_mode = True`, TM1py submits TI processes asynchronously and polls the TM1 server until they complete. Rather than polling at a fixed interval, TM1py uses **exponential backoff** — starting with frequent polls and gradually slowing down:

```
Poll 1: wait 0.1s → Poll 2: wait 0.2s → Poll 3: wait 0.4s → Poll 4: wait 0.8s → Poll 5+: wait 1.0s (cap)
```

This means short-running processes get detected quickly (within ~0.1s of completion), while long-running processes don't flood the server with polling requests.

You can tune the polling behavior per instance in `config.ini`:

```ini
[tm1-finance]
address = tm1server.company.com
port = 12354
ssl = True
async_requests_mode = True
async_polling_initial_delay = 0.1
async_polling_max_delay = 1.0
async_polling_backoff_factor = 2
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `async_polling_initial_delay` | `0.1` | Seconds to wait before the first poll |
| `async_polling_max_delay` | `1.0` | Maximum seconds between polls (cap) |
| `async_polling_backoff_factor` | `2` | Multiplier applied to the delay after each poll |

!!! tip "When to Adjust Polling"
    For workflows dominated by very short TI processes (under 1 second), keep the defaults — the 0.1s initial delay detects completion quickly. For workflows with mostly long-running processes (minutes), you can increase `async_polling_initial_delay` to `0.5` and `async_polling_max_delay` to `5.0` to reduce polling overhead on the TM1 server.

### Connection Recovery (RemoteDisconnect Retry)

Network interruptions between RushTI and the TM1 server can cause `RemoteDisconnected` errors mid-execution. TM1py handles these automatically with **exponential backoff retries** — reconnecting without failing the task:

```
Attempt 1: wait 1s → Attempt 2: wait 2s → Attempt 3: wait 4s → Attempt 4: wait 8s → Attempt 5: wait 16s
```

If the connection is restored within any retry window, the request completes normally. If all retries are exhausted, the task fails (and RushTI's own retry logic takes over if `retries > 0`).

Configure retry behavior per instance in `config.ini`:

```ini
[tm1-finance]
address = tm1server.company.com
port = 12354
ssl = True
remote_disconnect_max_retries = 5
remote_disconnect_retry_delay = 1.0
remote_disconnect_max_delay = 30
remote_disconnect_backoff_factor = 2
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `remote_disconnect_max_retries` | `5` | Maximum number of reconnection attempts |
| `remote_disconnect_retry_delay` | `1.0` | Seconds to wait before the first retry |
| `remote_disconnect_max_delay` | `30` | Maximum seconds between retries (cap) |
| `remote_disconnect_backoff_factor` | `2` | Multiplier applied to the delay after each retry |

!!! warning "Connection Recovery vs Task Retry"
    TM1py's connection recovery and RushTI's task retry (`retries` setting) are independent. Connection recovery handles transient network blips transparently — the task never knows the connection dropped. Task retry re-executes the entire TI process from scratch after a complete failure. For maximum resilience, use both: connection recovery handles brief outages, and task retry handles longer disruptions.

### Reducing Connection Overhead

For workflows with many short-running tasks, connection setup time can dominate execution time. To reduce this:

- Group related tasks on the same TM1 instance when possible.
- Keep `max_workers` aligned with the number of TM1 instances to avoid creating excess connections that are rarely used.

---

## Task Ordering

The order in which RushTI picks ready tasks matters. When multiple tasks are ready to run (all predecessors complete), RushTI must choose which to start first.

### Default Behavior

Without optimization, ready tasks are started in the order they appear in the task file.

### EWMA Optimization: Configurable Scheduling Algorithms

When optimization is enabled via `--optimize <algorithm>` or a JSON taskfile `optimization_algorithm` setting, RushTI sorts ready tasks by estimated runtime using EWMA (Exponentially Weighted Moving Average) estimates from historical execution data.

Two scheduling algorithms are available:

| Algorithm | Sort Order | Best For |
|-----------|-----------|----------|
| `longest_first` | Descending (longest tasks start first) | Independent workloads with varied task durations. Minimizes total makespan by starting expensive tasks early. |
| `shortest_first` | Ascending (shortest tasks start first) | Shared-resource TM1 workloads where concurrent heavy tasks cause contention. Reduces resource pressure by completing quick tasks first. |

**Why longest-first works:** If a 10-minute task and a 1-minute task are both ready, starting the 10-minute task first means the 1-minute task runs in parallel during those 10 minutes. Starting the 1-minute task first wastes 9 minutes of potential parallelism.

**Why shortest-first works:** When tasks share TM1 server resources (memory, threads, write locks), running many heavy tasks simultaneously causes contention. Starting short tasks first reduces the number of concurrent heavy tasks, lowering resource pressure and improving overall throughput.

### Enabling Optimization

Optimization is off by default. Activate it per-run via the CLI or per-taskfile via JSON settings:

```bash
# CLI: opt-in to shortest-first scheduling
rushti run --tasks daily-etl.json --max-workers 20 --optimize shortest_first

# CLI: opt-in to longest-first scheduling
rushti run --tasks daily-etl.json --max-workers 20 --optimize longest_first
```

Or set a default algorithm in a JSON task file:

```json
{
  "settings": {
    "optimization_algorithm": "shortest_first"
  }
}
```

CLI `--optimize` overrides the JSON setting. Omitting both means no optimization.

The EWMA tuning parameters live in `settings.ini` (system-wide, rarely changed):

```ini
# settings.ini
[stats]
enabled = true

[optimization]
lookback_runs = 10
min_samples = 3
```

The optimizer needs historical data, so stats must be enabled. After `min_samples` runs, the optimizer has enough data to produce reliable estimates.

### Manual Optimization with rushti stats analyze

If you prefer to control task ordering yourself, use the analyze command to generate an optimized task file:

```bash
# Analyze the last 20 runs and generate an optimized task file
rushti stats analyze \
  --workflow daily-etl \
  --tasks daily-etl.json \
  --output daily-etl-optimized.json \
  --runs 20

# Validate and use the optimized file
rushti tasks validate --tasks daily-etl-optimized.json --skip-tm1-check
rushti run --tasks daily-etl-optimized.json
```

The optimized task file reorders tasks so that long-running ones appear first while preserving all dependency constraints.

---

## Stages for Resource Control

Stages provide a second level of concurrency control beyond `max_workers`. Use `stage_workers` to limit parallelism during resource-intensive phases.

### Pattern: Aggressive Extract, Conservative Load

```json
{
  "settings": {
    "max_workers": 20,
    "stage_order": ["extract", "transform", "load"],
    "stage_workers": {
      "extract": 20,
      "transform": 12,
      "load": 4
    }
  }
}
```

- **Extract** (read-only): Use all 20 workers. Reads rarely cause contention.
- **Transform** (mixed): Moderate concurrency. Some transforms involve temporary cube writes.
- **Load** (write-heavy): Only 4 concurrent loads to avoid write lock contention and TM1 server memory pressure.

### Pattern: Staged Throttling

```json
{
  "settings": {
    "stage_workers": {
      "critical": 2,
      "normal": 16
    }
  }
}
```

Use a `critical` stage with low concurrency for tasks that modify shared dimensions or control cubes, and a `normal` stage for everything else.

---

## Timeout Strategies

Timeouts prevent runaway TI processes from blocking the entire pipeline.

### Setting Per-Task Timeouts

Analyze historical execution times from `rushti stats list tasks` and set timeouts to 2--3x the average duration:

```bash
# Check average task durations
rushti stats list tasks --workflow daily-etl

# If extract-gl averages 45s, set timeout to 120s
```

```json
{
  "id": "1",
  "process": "Finance.Extract.GLData",
  "timeout": 120
}
```

### When to Use cancel_at_timeout

- **Non-critical tasks**: Reports, exports, and notifications where partial data is acceptable.
- **Retry-safe tasks**: Tasks marked `safe_retry: true` that can be re-executed later.
- **Never for data loads**: Cancelling a write operation mid-execution can corrupt cube data.

```json
{
  "id": "5",
  "process": "Report.Generate.Optional",
  "timeout": 300,
  "cancel_at_timeout": true
}
```

---

## EWMA Tuning

The EWMA (Exponentially Weighted Moving Average) optimizer has several tuning parameters.

### Key Parameters

| Parameter | Default | Effect |
|-----------|---------|--------|
| `lookback_runs` | 10 | How many recent runs to consider. Higher = more stable estimates, slower to adapt. |
| `min_samples` | 3 | Minimum data points before optimization activates for a task. Lower = earlier activation but less reliable estimates. |
| `time_of_day_weighting` | false | Weight runs at similar times of day more heavily. Enable for workloads with time-dependent performance. |
| `cache_duration_hours` | 24 | How long to cache estimates. Shorter = more up-to-date but more I/O at startup. |

### Tuning with rushti stats analyze

The `--ewma-alpha` flag on `rushti stats analyze` controls how heavily recent runs are weighted:

| Alpha | Behavior |
|-------|----------|
| 0.1 | Very smooth -- emphasizes long-term average. Use for stable workloads. |
| 0.3 | Balanced (default). Good for most workloads. |
| 0.5 | Responsive -- adapts quickly to changes. Use for volatile workloads. |
| 0.8 | Aggressive -- almost entirely based on the most recent run. Use with caution. |

```bash
# Compare different alpha values
rushti stats analyze --workflow daily-etl --ewma-alpha 0.2 --report report_02.json
rushti stats analyze --workflow daily-etl --ewma-alpha 0.5 --report report_05.json
```

---

## Contention-Aware Analysis

When runtime-based scheduling is not enough, RushTI's contention-aware optimizer (`rushti stats optimize`) provides deeper analysis. See [Self-Optimization: Contention-Aware](../features/optimization.md#contention-aware-optimization) for the full algorithm description.

### When Contention Analysis Helps

| Symptom | What Contention Analysis Does |
|---------|-------------------------------|
| Adding workers does not reduce total runtime | Detects concurrency ceiling and recommends the optimal worker count |
| A few heavy tasks slow down everything when running together | Identifies heavy outlier groups and chains them sequentially |
| Reducing workers actually improved performance | Confirms the ceiling with multi-run comparison data |
| Unclear which parameter drives the performance difference | Identifies the contention driver (e.g., `pRegion`, `pDimension`) |

### Tuning Sensitivity

The `--sensitivity` parameter controls how aggressively outliers are detected:

| Sensitivity | Behavior |
|-------------|----------|
| `5.0` | Aggressive -- flags more groups as heavy. Use when you know contention is a problem. |
| `10.0` | Balanced (default). Good for most workloads. |
| `20.0` | Conservative -- only flags extreme outliers. Use for workflows with naturally varied durations. |

```bash
# Compare sensitivity levels
rushti stats optimize --workflow daily-etl --sensitivity 5.0
rushti stats optimize --workflow daily-etl --sensitivity 20.0
```

### Concurrency Ceiling vs Scale-Up

The optimizer detects two complementary signals from multi-run data:

- **Concurrency ceiling**: Runs with fewer workers were *faster*. The server was overwhelmed. The optimizer recommends reducing `max_workers`.
- **Scale-up opportunity**: Runs with more workers were *faster*, but the most recent run used fewer workers. The optimizer recommends increasing `max_workers` back to the efficient sweet spot.

The **sweet spot algorithm** avoids overreacting: it finds the fewest workers within 10% of the best observed wall clock time. For example, if 10 workers achieved 581s and 50 workers achieved 547s (only 6% faster), the optimizer recommends 10 workers — nearly the same speed with 5x fewer resources.

### Combining Contention Analysis with Stages

For complex workflows, combine contention-aware optimization with stage-based throttling:

```bash
# 1. Run contention analysis to understand the bottleneck
rushti stats optimize --workflow complex-etl --tasks complex-etl.json --output optimized.json

# 2. Review the HTML report for heavy groups and recommended workers
# 3. Add stage_workers constraints for resource-intensive phases
# 4. Run with the optimized file
rushti run --tasks optimized.json
```

The optimized task file embeds the recommended `max_workers` value. You can further refine it by adding stage-level worker limits.

---

## Monitoring and Identifying Bottlenecks

### Dashboard Visualization

The `rushti stats visualize` command generates an interactive HTML dashboard with:

- **Gantt chart**: Shows when each task started and ended. Look for gaps (idle workers) and long sequential chains.
- **Success rate trends**: Identifies flaky tasks that fail intermittently.
- **Duration trends**: Spots tasks that are getting slower over time.

```bash
rushti stats visualize --workflow daily-etl --runs 10
```

### DAG Visualization

The `rushti tasks visualize` command shows the dependency graph. Look for:

- **Over-specified dependencies**: Tasks that depend on more predecessors than necessary, creating artificial bottlenecks.
- **Long critical paths**: The longest chain of sequential dependencies determines the minimum possible runtime.
- **Fan-in bottlenecks**: A single task that depends on many predecessors and cannot start until the slowest one finishes.

```bash
rushti tasks visualize --tasks daily-etl.json --output dag.html --show-parameters
```

### What to Look For

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Gantt chart shows many gaps | Over-specified dependencies | Reduce predecessor lists to only true dependencies |
| One task takes 80% of total time | Single bottleneck task | Split the TI process into smaller units that can run in parallel |
| Tasks wait at stage boundaries | Stage concurrency too low | Increase `stage_workers` for the bottleneck stage |
| Workers idle at the end | Short tail tasks after a long critical path | Reorder tasks so long ones start first (enable optimization) |
| Tasks fail intermittently | TM1 server overload | Reduce `max_workers` or stagger with stages |

---

## Common Execution Patterns

### Fan-Out / Fan-In

Extract data from multiple regions in parallel, then consolidate into a single result:

```json
{
  "tasks": [
    { "id": "1", "process": "Extract.Regional", "parameters": { "pRegion": "NA" } },
    { "id": "2", "process": "Extract.Regional", "parameters": { "pRegion": "EU" } },
    { "id": "3", "process": "Extract.Regional", "parameters": { "pRegion": "APAC" } },
    {
      "id": "4",
      "process": "Consolidate.Global",
      "predecessors": ["1", "2", "3"],
      "require_predecessor_success": true
    }
  ]
}
```

**Tuning tip:** The fan-in task (task `4`) cannot start until the slowest extract finishes. Focus optimization on making the slowest extract faster.

### Pipeline (ETL Stages)

Sequential stages where each stage must complete before the next begins:

```json
{
  "settings": {
    "stage_order": ["extract", "transform", "load", "validate"]
  },
  "tasks": [
    { "id": "1", "stage": "extract", "process": "Extract.Sales" },
    { "id": "2", "stage": "extract", "process": "Extract.Costs" },
    { "id": "3", "stage": "transform", "process": "Transform.Data" },
    { "id": "4", "stage": "load", "process": "Load.Cube" },
    { "id": "5", "stage": "validate", "process": "Validate.Results" }
  ]
}
```

**Tuning tip:** If the extract stage takes 2 minutes and the transform stage takes 30 seconds, the pipeline is extract-bound. Focus on parallelizing more extract tasks.

### Diamond Dependencies

Two tasks that share common predecessors and a common successor:

```json
{
  "tasks": [
    { "id": "1", "process": "Setup.Environment" },
    { "id": "2", "process": "Process.PathA", "predecessors": ["1"] },
    { "id": "3", "process": "Process.PathB", "predecessors": ["1"] },
    { "id": "4", "process": "Merge.Results", "predecessors": ["2", "3"] }
  ]
}
```

**Tuning tip:** Tasks `2` and `3` run in parallel. The merge (task `4`) waits for both. If one path is much slower, consider splitting it further.

---

## Performance Checklist

Use this checklist when reviewing a workflow for performance:

- [ ] **Worker count**: Set to match TM1 server capacity, not arbitrarily high.
- [ ] **Dependencies**: Every predecessor relationship is truly necessary (no redundant edges).
- [ ] **Optimization enabled**: `[stats] enabled = true` in settings.ini, then `--optimize <algorithm>` on the CLI or `optimization_algorithm` in JSON taskfile.
- [ ] **Algorithm chosen**: Use `shortest_first` for shared-resource TM1 workloads, `longest_first` for independent tasks with varied durations.
- [ ] **Timeouts set**: All tasks have appropriate timeouts to prevent hangs.
- [ ] **Stages used**: Resource-intensive phases have limited `stage_workers`.
- [ ] **Dashboard reviewed**: Gantt chart shows minimal idle time between tasks.
- [ ] **Retry count appropriate**: Non-zero retries for transient failures, zero for logic errors.
- [ ] **Expandable parameters**: Dynamic member lists instead of hardcoded task duplication.
- [ ] **Contention analyzed**: Run `rushti stats optimize` after collecting runs at 2--3 worker levels. Review the HTML report for bottleneck insights.

---

## Next Steps

- **[Self-Optimization](../features/optimization.md)** -- Runtime scheduling and contention-aware analysis
- **[Settings Reference](settings-reference.md)** -- `[optimization]` and `[stats]` configuration
- **[Advanced Task Files](advanced-task-files.md)** -- Stages, timeouts, and expandable parameters
- **[CLI Reference](cli-reference.md)** -- `rushti stats analyze`, `rushti stats optimize`, and `rushti stats visualize`
