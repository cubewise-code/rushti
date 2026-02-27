# Settings Reference

Complete reference for `config/settings.ini`. RushTI reads this file at startup and applies its values as defaults. CLI flags and JSON task file settings take precedence over values defined here.

---

## Settings Precedence

RushTI resolves each setting from multiple sources using a strict priority order:

```
CLI arguments > JSON task file settings > settings.ini > Built-in defaults
```

When the same setting appears in multiple sources, the highest-priority source wins. For example, `--max-workers 16` on the command line overrides `"max_workers": 12` in the task file, which overrides `max_workers = 8` in `settings.ini`, which overrides the built-in default of `4`.

!!! tip "Debugging Settings Resolution"
    Run with `--log-level DEBUG` to see exactly which source each setting was resolved from:
    ```bash
    rushti run --tasks tasks.json --log-level DEBUG
    ```

---

## File Location

RushTI searches for `settings.ini` in the following order:

1. Path specified via `--settings` / `-s` CLI argument
2. `RUSHTI_DIR` environment variable (looks for `config/settings.ini` under this directory)
3. `./settings.ini` (current directory -- legacy location, deprecation warning emitted)
4. `./config/settings.ini` (recommended location)

If no file is found, built-in defaults are used for all settings.

---

## Configuration Sections

### [defaults]

Common execution settings that control basic RushTI behavior.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `max_workers` | int | `4` | Maximum number of parallel workers. Valid range: 1--100. Recommended: start at 4, increase based on TM1 server capacity. |
| `retries` | int | `0` | Number of retry attempts for failed TI process executions. Valid range: 0--10. Retries use exponential backoff (1s, 2s, 4s, ...). |
| `result_file` | str | `""` (empty) | CSV output path for execution summary. Empty string means no CSV is created. |
| `mode` | str | `norm` | **Deprecated.** Execution mode is now auto-detected from file content. JSON files always use DAG execution; TXT files use the mode indicated by their content structure. Kept for backward compatibility only. |

**Overridable via CLI:** `--max-workers` / `-w`, `--retries` / `-r`, `--result` / `-o`, `--mode` / `-m` (deprecated)

**Overridable via JSON task file:** `max_workers`, `retries`, `result_file`

---

### [optimization]

EWMA tuning parameters for task runtime estimation. These settings control how the optimizer calculates runtime estimates from historical execution data. They are system-wide and rarely changed.

Optimization itself is **off by default** and activated per-taskfile via:

- **CLI:** `--optimize longest_first` or `--optimize shortest_first`
- **JSON task file:** `"optimization_algorithm": "shortest_first"` in the settings section

CLI `--optimize` overrides the JSON taskfile setting. Omitting both means no optimization.

**Prerequisite:** Requires `[stats] enabled = true` to collect the historical execution data that optimization needs.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `lookback_runs` | int | `10` | Number of recent successful runs to consider when computing EWMA estimates. Valid range: 1--100. |
| `time_of_day_weighting` | bool | `false` | Weight historical runs at similar times of day more heavily. Useful for processes whose performance varies by time of day (e.g., overnight batch vs. daytime). When enabled, the estimate cache is disabled and recalculated each run. |
| `min_samples` | int | `3` | Minimum number of successful historical samples required before optimization activates for a task. Tasks with fewer samples use a default 10-second estimate. |
| `cache_duration_hours` | int | `24` | Hours to cache EWMA estimates between runs. Ignored when `time_of_day_weighting` is enabled. |

**Overridable via CLI:** `--optimize <algorithm>` (activates optimization with the chosen scheduling algorithm)

**Overridable via JSON task file:** `optimization_algorithm` (string: `"longest_first"` or `"shortest_first"`)

!!! note "How EWMA Works"
    EWMA (Exponentially Weighted Moving Average) gives more weight to recent execution times while smoothing out outliers. The `lookback_runs` value determines how many historical data points are considered. A higher value produces more stable estimates; a lower value adapts faster to changes.

---

### Logging

Logging is configured exclusively through `config/logging_config.ini`, which uses Python's standard `logging.config.fileConfig` format. This file controls log level, handlers, formatters, and file rotation.

By default, RushTI writes `rushti.log` to the application root directory (next to the executable or under `RUSHTI_DIR`). The log level can be overridden at runtime with `--log-level` / `-L`.

See `config/logging_config.ini` for the full configuration.

#### Log File Path Resolution

Relative file paths in `logging_config.ini` (e.g., `'rushti.log'`) are automatically resolved against the **application directory** — not the current working directory. This means the log file is always created in the expected location, even when RushTI is invoked from a different directory (e.g., via TM1's `ExecuteCommand` where the working directory is typically `C:\windows\system32`).

The application directory is determined by:

1. `RUSHTI_DIR` environment variable (if set)
2. The directory containing the executable (for standalone `.exe` builds)
3. The project root (for pip-installed scripts)

If you specify an **absolute path** in `logging_config.ini` (e.g., `'E:/logs/rushti.log'`), it is used as-is.

!!! tip "Choosing a Log Level"
    - **DEBUG**: Full worker activity, task scheduling decisions, connection details. Use for troubleshooting.
    - **INFO**: Normal operations -- task starts/completions, run summary. Default for production.
    - **WARNING**: Only issues that may need attention (deprecated features, non-blocking failures).
    - **ERROR**: Only failures that affect task execution.

---

### [tm1_integration]

Controls TM1-based read/write integration: reading task files from a TM1 cube and pushing execution results back to TM1.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `push_results` | bool | `false` | Upload the results CSV to the TM1 Applications folder after each run. The file is named `rushti_{workflow}_{run_id}.csv` (with `.blb` extension for TM1 < v12). |
| `auto_load_results` | bool | `false` | After uploading results (requires `push_results = true`), call the `}rushti.load.results` TI process on the target TM1 instance to load the CSV into the rushti cube. Passes `pSourceFile` and `pTargetCube` parameters. The process must exist on the target instance. |
| `default_tm1_instance` | str | *(none)* | Default TM1 instance name (from `config.ini`) used for reading task files and writing results. Required when `push_results` is enabled. |
| `default_rushti_cube` | str | `rushti` | Name of the TM1 cube for task definitions and execution results. Created by the `rushti build` command. |
| `default_workflow_dim` | str | `rushti_workflow` | Dimension name for workflow identifiers. |
| `default_task_id_dim` | str | `rushti_task_id` | Dimension name for task sequence elements (1--5000 default elements). |
| `default_run_id_dim` | str | `rushti_run_id` | Dimension name for run timestamps (`YYYYMMDD_HHMMSS`) plus an `Input` element for definitions. |
| `default_measure_dim` | str | `rushti_measure` | Dimension name for log field measures. |

**Overridable via CLI:** `--tm1-instance`, `--workflow` / `-W`

**Overridable via JSON task file:** `push_results`, `auto_load_results`

!!! info "Setting Up TM1 Integration"
    Run `rushti build --tm1-instance <instance>` to create the required dimensions and cube automatically before enabling `push_results`.

---

### [exclusive_mode]

Prevents concurrent RushTI executions by checking TM1 server sessions for other RushTI instances.

RushTI identifies sessions using the TM1 session context field:

- **Normal mode:** `RushTI_{workflow}`
- **Exclusive mode:** `RushTIX_{workflow}`

**Interaction matrix:**

| Current Run | Other Session | Result |
|-------------|---------------|--------|
| Exclusive | Any RushTI session running | Wait |
| Non-exclusive | Exclusive session running | Wait |
| Non-exclusive | Non-exclusive session running | Proceed |

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `enabled` | bool | `false` | Enable exclusive mode by default for all runs. Can also be enabled per-taskfile via JSON settings or `--exclusive` flag. |
| `polling_interval` | int | `30` | Seconds between TM1 session checks while waiting for exclusive access. Valid range: 5--300. |
| `timeout` | int | `600` | Maximum seconds to wait for exclusive access before the run fails with `ExclusiveModeTimeoutError`. Valid range: 0--3600. |

**Overridable via CLI:** `--exclusive` / `-x`, `--force` / `-f` (bypasses checks)

**Overridable via JSON task file:** `exclusive` (boolean in settings section)

---

### [resume]

Controls checkpoint saving and resume capability. When enabled, RushTI periodically saves execution state so that interrupted runs can be resumed from the point of failure.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `enabled` | bool | `false` | Enable automatic checkpoint saving during execution. |
| `checkpoint_interval` | int | `60` | Seconds between checkpoint saves. Valid range: 10--600. Lower values provide finer recovery granularity but add I/O overhead. |
| `checkpoint_dir` | str | `./checkpoints` | Directory where checkpoint JSON files are stored. Created automatically if it does not exist. |
| `auto_resume` | bool | `false` | Automatically resume from the last checkpoint on restart. When `false`, use the `rushti resume` command to manually resume. |

**Overridable via CLI:** `--no-checkpoint` (disables for the current run)

!!! tip "Checkpoint Files"
    Checkpoint files are named `checkpoint_{workflow}_{timestamp}.json` and contain the full execution state: completed tasks, in-progress tasks, pending tasks, and their results. Use `rushti resume --checkpoint <file>` to resume from a specific checkpoint.

---

### [stats]

Controls the SQLite statistics database that stores execution history. The stats database powers several features: EWMA optimization, the `rushti stats` commands, dashboard visualization, and historical analysis.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `enabled` | bool | `false` | Enable the SQLite stats database. When enabled, every run records task-level execution data (timing, status, errors). |
| `retention_days` | int | `90` | Days to keep execution history. Records older than this value are deleted at startup. Valid range: 1--365. Use `0` to keep data indefinitely. |

**Required by:** `[optimization]`, `rushti stats` commands, `rushti stats visualize`

---

## Complete settings.ini Template

Copy this template to `config/settings.ini` and uncomment the settings you want to change:

```ini
# RushTI Settings Configuration
# ==============================
#
# Settings Precedence (highest to lowest):
# 1. CLI arguments (e.g., --max-workers 8)
# 2. JSON task file settings section
# 3. This settings.ini file
# 4. Built-in defaults

# ------------------------------------------------------------------------------
# [defaults] - Common execution settings
# ------------------------------------------------------------------------------
[defaults]

# Maximum number of parallel workers
# Valid range: 1-100 (recommended: start at 4, increase based on TM1 capacity)
# Default: 4
# max_workers = 4

# Number of retries for failed process executions
# Valid range: 0-10
# Default: 0
# retries = 0

# Output file path for execution results summary CSV
# Leave empty or omit to skip creating the summary CSV
# Default: (empty - no CSV created)
# result_file = rushti.csv

# Execution mode (deprecated - auto-detected from file content)
# Default: norm
# mode = norm

# ------------------------------------------------------------------------------
# [optimization] - EWMA tuning parameters for task runtime estimation
# ------------------------------------------------------------------------------
[optimization]

# These settings control how the optimizer calculates runtime estimates
# from historical execution data. They are system-wide and rarely changed.
#
# Optimization is activated per-taskfile via:
#   CLI:  --optimize longest_first  or  --optimize shortest_first
#   JSON: "optimization_algorithm": "shortest_first" in settings section
#
# Requires: [stats] enabled = true (needs historical data for estimates)

# Number of historical runs to analyze for EWMA runtime estimates
# Valid range: 1-100
# Default: 10
# lookback_runs = 10

# Weight historical runs at similar times of day higher
# Useful for processes with time-varying performance
# Note: When enabled, caching is disabled (recalculates each run)
# Default: false
# time_of_day_weighting = false

# Minimum number of successful samples required before applying optimization
# Tasks with fewer samples use a default estimate
# Default: 3
# min_samples = 3

# Hours to cache optimization data between runs
# Ignored if time_of_day_weighting is enabled
# Default: 24
# cache_duration_hours = 24

# ------------------------------------------------------------------------------
# [tm1_integration] - TM1 integration for reading taskfiles and logging results
# ------------------------------------------------------------------------------
[tm1_integration]

# Push execution results to TM1
# When enabled, the results CSV is uploaded to TM1 files after each run
# as: rushti_{workflow}_{run_id}.csv
#
# To set up TM1 integration:
# 1. Run: rushti build --tm1-instance tm1srv01
#    This creates the required dimensions and cube automatically.
# 2. Set push_results = true and configure default_tm1_instance below
#
# Default: false
# push_results = false

# Automatically load results into TM1 cube after push
# When enabled (and push_results = true), calls }rushti.load.results TI process
# on the target TM1 instance after uploading the results CSV.
# The TI process must exist on the target instance.
#
# Default: false
# auto_load_results = false

# Default TM1 instance for reading taskfiles and writing results
# Must be defined in config.ini
# default_tm1_instance = tm1srv01

# Default cube name for task definitions and results
# Default: rushti
# default_rushti_cube = rushti

# Default dimension names (must match the objects created by rushti build)
# default_workflow_dim = rushti_workflow
# default_task_id_dim = rushti_task_id
# default_run_id_dim = rushti_run_id
# default_measure_dim = rushti_measure

# ------------------------------------------------------------------------------
# [exclusive_mode] - Prevent concurrent RushTI executions
# ------------------------------------------------------------------------------
[exclusive_mode]

# Enable exclusive mode checking by default
# When enabled, RushTI will check TM1 server sessions for other RushTI instances
# and wait if an exclusive session is running.
#
# Can also be enabled per-taskfile via JSON settings.exclusive or --exclusive flag
# Default: false
# enabled = false

# Seconds between checks when waiting for exclusive access
# Valid range: 5-300
# Default: 30
# polling_interval = 30

# Maximum seconds to wait for exclusive access before failing
# Valid range: 0-3600
# Default: 600 (10 minutes)
# timeout = 600

# ------------------------------------------------------------------------------
# [resume] - Checkpoint and resume feature
# ------------------------------------------------------------------------------
[resume]

# Enable checkpoint saving for resume capability
# Default: false
# enabled = false

# Seconds between checkpoint saves
# Valid range: 10-600
# Default: 60
# checkpoint_interval = 60

# Directory for checkpoint files
# Default: ./checkpoints
# checkpoint_dir = ./checkpoints

# Automatically resume from last checkpoint on startup
# Default: false
# auto_resume = false

# ------------------------------------------------------------------------------
# [stats] - SQLite stats database for execution history
# ------------------------------------------------------------------------------
[stats]

# Enable the stats database for storing execution history
# The stats database stores execution statistics for:
# - Optimization features (EWMA runtime estimation)
# - TM1 cube logging data source
# - Historical analysis via 'rushti stats' commands
# Default: false
# enabled = false

# Path to the SQLite database file
# Relative paths are resolved from the application directory
# Default: data/rushti_stats.db
# db_path = data/rushti_stats.db

# Number of days to retain execution history
# Valid range: 1-365
# Default: 90
# retention_days = 90
```

---

## Environment Variables

One environment variable controls RushTI's root directory for all files:

| Variable | Purpose |
|----------|---------|
| `RUSHTI_DIR` | Root directory for all RushTI files. Config files are read from `{RUSHTI_DIR}/config/`, and all output (logs, stats database, checkpoints, archives, visualizations) is written under this directory. |

```bash
export RUSHTI_DIR=/opt/rushti/prod/

rushti run --tasks daily-etl.json
# Config:         /opt/rushti/prod/config/config.ini, settings.ini, logging_config.ini
# Logs:           /opt/rushti/prod/rushti.log
# Stats DB:       /opt/rushti/prod/data/rushti_stats.db
# Checkpoints:    /opt/rushti/prod/checkpoints/
# Archives:       /opt/rushti/prod/archive/{workflow}/{run_id}.json
# Visualizations: /opt/rushti/prod/visualizations/
```

---

## Boolean Values

All boolean settings accept the following values (case-insensitive):

| True | False |
|------|-------|
| `true`, `yes`, `1`, `on` | `false`, `no`, `0`, `off` |

---

## Validation

RushTI validates all settings at load time:

- **Unknown sections** produce a warning (possible typo) but do not cause failure.
- **Unknown keys** within known sections produce a warning.
- **Type mismatches** (e.g., `max_workers = abc`) cause an immediate error.
- **Out-of-range values** (e.g., `max_workers = 0`) cause an immediate error.
- **Invalid enum values** (e.g., `level = TRACE`) cause an immediate error.

---

## Next Steps

- **[CLI Reference](cli-reference.md)** -- Command-line options that override these settings
- **[Advanced Task Files](advanced-task-files.md)** -- JSON task file settings section
