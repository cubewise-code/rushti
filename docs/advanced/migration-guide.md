# Migration from v1.x

Guide for upgrading from RushTI 1.x to 2.0. Your existing task files continue to work -- RushTI 2.0 is fully backward compatible with the TXT format.

---

## What's New in v2.0

RushTI 2.0 adds major features while preserving backward compatibility:

| Feature | Description |
|---------|-------------|
| **JSON task files** | Rich metadata, typed parameters, inline settings, and stages |
| **DAG execution** | Fine-grained task-level dependencies instead of level-based wait barriers |
| **Self-optimization** | EWMA-based runtime estimation reorders tasks for maximum parallelism |
| **SQLite stats database** | Persistent execution history for analysis and optimization |
| **Exclusive mode** | Prevents concurrent RushTI sessions using TM1 session context |
| **Checkpoint and resume** | Periodic state snapshots allow interrupted runs to resume from where they stopped |
| **TM1 integration** | Read task files from a TM1 cube and push results back |
| **Stages** | Group tasks into sequential pipeline phases with per-stage worker limits |
| **Expandable parameters** | MDX expressions that generate tasks dynamically from TM1 dimensions |
| **Interactive dashboards** | HTML visualizations of DAGs and execution Gantt charts |

---

## What Changed

### CLI: New Subcommand Structure

v2.0 introduces a structured CLI with command groups. The old positional syntax continues to work.

| v1.x Syntax | v2.0 Equivalent | Notes |
|-------------|-----------------|-------|
| `rushti.py tasks.txt 4` | `rushti run --tasks tasks.txt --max-workers 4` | `run` is the default command |
| `rushti.py tasks.txt 4 opt 2 result.csv` | `rushti run -t tasks.txt -w 4 -r 2 -o result.csv` | Positional syntax still works |
| *(not available)* | `rushti tasks expand --tasks t.json -o expanded.json` | New: MDX expansion |
| *(not available)* | `rushti tasks visualize --tasks t.json -o dag.html` | New: DAG visualization |
| *(not available)* | `rushti tasks validate --tasks t.json --skip-tm1-check` | New: task file validation |
| *(not available)* | `rushti stats list runs -W daily-etl` | New: execution history |
| *(not available)* | `rushti stats export -W daily-etl -o results.csv` | New: export to CSV |
| *(not available)* | `rushti stats visualize -W daily-etl` | New: HTML dashboard |
| *(not available)* | `rushti stats analyze -W daily-etl -t t.json -o opt.json` | New: optimization |
| *(not available)* | `rushti build --tm1-instance tm1srv01` | New: create TM1 logging objects |
| *(not available)* | `rushti resume --checkpoint checkpoint.json` | New: resume from checkpoint |
| *(not available)* | `rushti db list workflows` | New: database administration |
| *(not available)* | `rushti db vacuum` | New: compact database |

### Settings: New config/settings.ini

v1.x had no settings file. v2.0 introduces `config/settings.ini` with six sections:

| Section | Purpose |
|---------|---------|
| `[defaults]` | `max_workers`, `retries`, `result_file`, `mode` |
| `[optimization]` | EWMA self-optimization |
| `[tm1_integration]` | TM1 read/write integration |
| `[exclusive_mode]` | Concurrent execution prevention |
| `[resume]` | Checkpoint and resume |
| `[stats]` | SQLite execution history |

Logging is configured separately via `config/logging_config.ini` (Python's standard `logging.config.fileConfig` format).

All settings have sensible defaults. You do not need to create a `settings.ini` to use v2.0 -- it works with built-in defaults.

### --mode Flag: Auto-Detected for Files, Still Required for Cube Reads

In v1.x, the `--mode` flag (`norm` or `opt`) controlled whether RushTI used level-based (wait) or dependency-based execution.

In v2.0, mode is **auto-detected from file content** for file sources (`--tasks`):

- JSON files always use DAG execution (optimized mode).
- TXT files use `norm` mode if they contain `wait` keywords, or `opt` mode if tasks have `id` and `predecessors` fields.

For file sources the `--mode` flag is therefore accepted for backward compatibility but ignored.

!!! warning "Cube reads still need `--mode`"
    Auto-detection does **not** apply when reading from a TM1 cube (`--tm1-instance`). The cube stores every workflow with the same measures, so RushTI cannot tell a wait-based workflow from a predecessor-based one. Cube reads default to `norm` (wait-based) and **ignore the `predecessors` measure** unless you pass `--mode opt`. See [TM1 integration → Choosing the mode for cube reads](../features/tm1-integration.md#choosing-the-mode-for-cube-reads).

### Task File: id and predecessors in TXT Files

v2.0 TXT files support `id=` and `predecessors=` fields to enable DAG execution without migrating to JSON:

**v1.x TXT (level-based):**

```
instance=tm1srv01 process=Extract.Data pYear=2026
instance=tm1srv01 process=Extract.Costs pYear=2026
wait
instance=tm1srv01 process=Transform.Data pYear=2026
wait
instance=tm1srv01 process=Load.Cube pYear=2026
```

**v2.0 TXT (DAG-based):**

```
id=1 instance=tm1srv01 process=Extract.Data pYear=2026
id=2 instance=tm1srv01 process=Extract.Costs pYear=2026
id=3 instance=tm1srv01 process=Transform.Data pYear=2026 predecessors=1,2
id=4 instance=tm1srv01 process=Load.Cube pYear=2026 predecessors=3
```

### No Breaking Changes

- Existing TXT task files work without modification.
- The old positional CLI syntax (`rushti.py tasks.txt 4 opt 2`) still works.
- The `config.ini` TM1 connection format is unchanged.

---

## Numeric `task_id` Required {#numeric-task_id-required}

Starting with the version that introduces `--detailed-results`, the JSON taskfile schema rejects non-integer `task_id` values. RushTI was always intended to use numeric IDs — the `rushti_task_id` cube dimension is pre-populated with integer-named elements `"1".."5000"` and result rows can only land in the cube if their `task_id` matches an element. Earlier releases were lax about this; the validator now enforces it explicitly.

**Accepted forms:**

- JSON integer: `"id": 5`
- Integer-shaped string: `"id": "5"`

**Rejected (now raises a `TaskfileValidationError` at parse time):**

| Bad value | Reason |
|-----------|--------|
| `"id": 0` | Zero is not a positive integer; cube dimension starts at 1 |
| `"id": -3` | Negatives have no cube element |
| `"id": "05"` | Leading-zero strings don't match the integer-named cube elements (`"5"` not `"05"`) |
| `"id": 5.0` | Floats are not integers |
| `"id": "task-1"` | Non-integer string |
| `"id": "abc"` | Non-integer string |

The same rule applies to entries in `predecessors`.

### How to Migrate Legacy Taskfiles

If your taskfile uses string IDs like `"task-1"`, `"extract-load"`, or `"e1"`, rewrite them as integers (`1`, `2`, `3`, ...) and update every `predecessors` reference accordingly. The order doesn't matter — pick whatever sequence is convenient.

```json
// Before
{
  "tasks": [
    {"id": "extract-1", "instance": "tm1srv01", "process": "..." },
    {"id": "transform-1", "predecessors": ["extract-1"], "instance": "tm1srv01", "process": "..." }
  ]
}

// After
{
  "tasks": [
    {"id": 1, "instance": "tm1srv01", "process": "..." },
    {"id": 2, "predecessors": [1], "instance": "tm1srv01", "process": "..." }
  ]
}
```

The error message you'll see if validation fails:

```
Task file validation failed:
  - Task 0: 'id' must be a positive integer. Got: 'extract-1'. Task IDs must be positive integers (the rushti_task_id cube dimension uses integer member names).
```

---

## TM1 Model Auto-Upgrade

`rushti build` is now non-destructive by default. When you upgrade RushTI to a release that adds new measure elements (e.g. `original_task_id`), the next `rushti build --tm1-instance <inst>` patches the existing model in place:

- **Dimensions.** Missing measure elements are added; existing elements and their attribute values are left alone. Cube data is preserved.
- **`}rushti.load.results`.** The TI process is application-owned and is always replaced with the latest body — TI processes are stateless, so this is safe.
- **`--force`.** Still wipes and rebuilds dimensions and the cube (data loss). Use only on dev/test environments.

You don't need a separate command — bare `rushti build` does the right thing. CI/CD pipelines that run `rushti build` on every release continue to work across version upgrades without manual intervention.

!!! warning "Re-run `rushti build` after every upgrade"
    `}rushti.load.results` maps result-CSV columns to its variables **by position**. If a release adds a measure column (as 2.3.0 did with `chore`) and you upgrade the Python package **without** re-running `rushti build`, the stale process reads each column into the wrong variable and writes values to the next measure over (issue [#169](https://github.com/cubewise-code/rushti/issues/169)). The loader now validates the CSV header and fails with a clear error instead of silently scrambling data — but the fix for a stale deployment is still to **re-run `rushti build --tm1-instance <inst>`**. If you already have scrambled rows, clear them and re-run the workflow.

---

## Step-by-Step Migration

### Phase 1: Install and Verify (Day 1)

Install RushTI 2.0 and verify your existing workflows still run.

```bash
# Install or upgrade
pip install --upgrade rushti

# Verify version
rushti --version

# Test existing TXT task file
rushti run --tasks Tasks/existing.txt --max-workers 4

# Verify output matches v1.x behavior
```

### Phase 2: Create Settings File (Day 1)

Create a minimal `settings.ini` from the provided template:

```bash
# Create config directory (if it doesn't exist)
mkdir -p config

# Copy template
cp config/settings.ini.template config/settings.ini
```

At minimum, enable stats tracking (zero-cost insight into your workflows):

```ini
[defaults]
max_workers = 4
retries = 0

[stats]
enabled = true
retention_days = 90
```

### Phase 3: Convert Task Files to JSON (Week 1)

JSON task files unlock the full feature set. Convert your TXT files:

```bash
# Export TXT to JSON
rushti tasks export --tasks Tasks/daily-etl.txt --output Tasks/daily-etl.json

# Add metadata to the JSON file (edit manually)
```

After conversion, add a `metadata` section to each JSON file:

```json
{
  "version": "2.0",
  "metadata": {
    "workflow": "daily-etl",
    "name": "Daily Financial ETL",
    "description": "Extracts, transforms, and loads financial data",
    "author": "TM1 Admin"
  },
  "tasks": [...]
}
```

The `workflow` is important -- it links stats, checkpoints, and analysis to a consistent identifier.

Validate each converted file:

```bash
rushti tasks validate --tasks Tasks/daily-etl.json --skip-tm1-check
```

### Phase 4: Enable New Features (Week 2)

Enable features incrementally. Each is independent -- enable only what you need.

**Checkpointing** (safety net for long-running workflows):

```ini
[resume]
enabled = true
checkpoint_interval = 60
checkpoint_dir = ./checkpoints
```

**Exclusive mode** (for production critical workflows):

```json
{
  "settings": {
    "exclusive": true
  }
}
```

Or globally in `settings.ini`:

```ini
[exclusive_mode]
enabled = true
timeout = 600
```

**Timeouts** (prevent runaway processes):

```json
{
  "id": "7",
  "process": "Report.Generate",
  "timeout": 300,
  "cancel_at_timeout": true
}
```

### Phase 5: Enable Optimization (Week 3--4)

After several runs have populated the stats database, opt in to optimization on your runs:

```bash
# Enable optimization with shortest-first scheduling (good for shared-resource TM1 workloads)
rushti run --tasks Tasks/daily-etl.json --max-workers 20 --optimize shortest_first

# Or set a default algorithm in a JSON task file
# "settings": { "optimization_algorithm": "shortest_first" }
```

The EWMA tuning parameters in `settings.ini` control how runtime estimates are calculated (rarely needs changing):

```ini
[optimization]
lookback_runs = 10
min_samples = 3
```

Monitor the effect:

```bash
# Check execution trends
rushti stats visualize --workflow daily-etl --runs 20

# Generate analysis report
rushti stats analyze --workflow daily-etl --runs 20
```

Typical improvement: 15--25% runtime reduction from better task ordering.

---

## Recommended Features to Enable

These three features provide immediate value with minimal configuration:

### 1. Stats Database (Free Insight)

**Effort:** Add 2 lines to `settings.ini`. **Benefit:** Execution history, dashboards, and the data foundation for optimization.

```ini
[stats]
enabled = true
retention_days = 90
```

### 2. Checkpointing (Safety Net)

**Effort:** Add 3 lines to `settings.ini`. **Benefit:** Resume interrupted runs instead of restarting from scratch. Especially valuable for workflows that take more than 10 minutes.

```ini
[resume]
enabled = true
checkpoint_interval = 60
```

### 3. EWMA Optimization (Performance)

**Effort:** Add `--optimize <algorithm>` to your run command or set `optimization_algorithm` in a JSON task file (after stats is enabled and you have at least 3 runs of history). **Benefit:** Automatic task scheduling that reduces total runtime.

```bash
rushti run --tasks daily-etl.json --max-workers 20 --optimize shortest_first
```

---

## Common Migration Issues

### Issue: "Config file not found: config.ini"

v2.0 expects `config/config.ini` by default.

```bash
# Move config files to the config directory
mkdir -p config
mv config.ini config/config.ini
mv settings.ini config/settings.ini
mv logging_config.ini config/logging_config.ini

# Or use RUSHTI_DIR to set the root directory for everything
export RUSHTI_DIR=/path/to/rushti-root/
# Config files are expected in: /path/to/rushti-root/config/
rushti run --tasks tasks.json
```

!!! note "Environment Variable Changes"
    v2.0 replaces the per-file environment variables (`RUSHTI_CONFIG`, `RUSHTI_SETTINGS`)
    with a single `RUSHTI_DIR` variable that sets the root directory for all RushTI files.
    Config files are expected in `{RUSHTI_DIR}/config/`, and all output (logs, stats database,
    checkpoints, visualizations) is written under `RUSHTI_DIR`.

    **Before (v1.x):**
    ```bash
    export RUSHTI_CONFIG=/etc/rushti/config.ini
    export RUSHTI_SETTINGS=/etc/rushti/settings.ini
    ```

    **After (v2.0):**
    ```bash
    export RUSHTI_DIR=/etc/rushti/
    # Expected structure:
    #   /etc/rushti/config/     - config.ini, settings.ini, logging_config.ini
    #   /etc/rushti/data/       - rushti_stats.db
    #   /etc/rushti/rushti.log
    #   /etc/rushti/checkpoints/
    #   /etc/rushti/visualizations/
    ```

### Issue: Only 4 Workers Despite Higher Setting

Settings precedence applies: CLI > task file > `settings.ini` > default (4).

```bash
# Verify which source is being used
rushti run --tasks tasks.json --log-level DEBUG

# Look for:
# DEBUG: Effective settings: workers=4, ...
```

Check that `max_workers` is set in the correct place with the correct spelling.

### Issue: Predecessors Missing When Running from a Cube

If you run an optimized workflow from the cube (`--tm1-instance`) and the resulting plan ignores your `predecessors`, you are running in the default `norm` mode. Add `--mode opt` to the command (or set `mode = opt` in `settings.ini`). Mode auto-detection applies to file sources only — see [TM1 integration → Choosing the mode for cube reads](../features/tm1-integration.md#choosing-the-mode-for-cube-reads). For `--tasks` file sources you can safely omit `--mode` entirely.

### Issue: TXT File Not Auto-Detecting Mode

TXT files must start with either `id=` (opt mode) or `instance=` (norm mode) for auto-detection. If auto-detection fails, the file is treated as norm mode.

---

## Feature Comparison

| Capability | v1.x | v2.0 |
|-----------|------|------|
| TXT task files | Yes | Yes (backward compatible) |
| JSON task files | No | Yes |
| Parallel execution | Yes (level-based) | Yes (DAG-based) |
| Wait barriers | Yes | Yes (auto-converted to DAG) |
| Per-task dependencies | No | Yes (`predecessors`) |
| Per-task timeouts | No | Yes (`timeout`, `cancel_at_timeout`) |
| Conditional execution | No | Yes (`require_predecessor_success`) |
| Stages | No | Yes |
| Dynamic parameters (MDX) | No | Yes (expandable parameters) |
| Retry with backoff | Yes | Yes |
| Result CSV | Yes | Yes |
| Checkpoint and resume | No | Yes |
| Exclusive mode | No | Yes |
| SQLite stats database | No | Yes |
| EWMA self-optimization | No | Yes |
| DAG visualization | No | Yes (interactive HTML) |
| Execution dashboard | No | Yes (interactive HTML) |
| TM1 cube read/write | No | Yes |
| Settings file | No | Yes (`settings.ini`) |
| Subcommand CLI | No | Yes |

---

## Rollback

If you encounter issues with v2.0, you can continue using v1.x behavior without downgrading:

```bash
# Use v1.x positional syntax (still supported)
rushti.py Tasks/my_tasks.txt 4 opt 2

# Disable all new features
# settings.ini
[stats]
enabled = false

[resume]
enabled = false

[exclusive_mode]
enabled = false

# Optimization is off by default (no --optimize flag needed)
```

---

## Next Steps

- **[Settings Reference](settings-reference.md)** -- Complete `settings.ini` reference
- **[CLI Reference](cli-reference.md)** -- All commands and options
- **[Advanced Task Files](advanced-task-files.md)** -- Stages, timeouts, expandable parameters
- **[Performance Tuning](performance-tuning.md)** -- Optimizing parallel execution
