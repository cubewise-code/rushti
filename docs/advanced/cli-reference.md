# CLI Reference

Compact reference for all RushTI commands. For detailed usage of any command, run `rushti <command> --help`.

---

## Command Structure

```
rushti [command] [subcommand] [options]
```

If no command is specified, `run` is assumed. This means `rushti --tasks tasks.json` and `rushti run --tasks tasks.json` are equivalent.

### Available Commands

| Command | Purpose |
|---------|---------|
| `run` | Execute a task file (default) |
| `resume` | Resume execution from a checkpoint |
| `tasks` | Task file operations: export, push, expand, visualize, validate |
| `stats` | Statistics queries: list, export, visualize, analyze |
| `build` | Create TM1 logging objects |
| `db` | Database administration: list, clear, show, vacuum |

### Global Options

These options are available on all commands:

| Option | Short | Description |
|--------|-------|-------------|
| `--help` | `-h` | Show help for the command |
| `--version` | `-v` | Show RushTI version |
| `--log-level` | `-L` | Override log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

---

## rushti run

Execute a task file with parallel workers and DAG-based scheduling.

### Syntax

```bash
rushti run --tasks FILE [options]
rushti --tasks FILE [options]          # 'run' is the default command
```

### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--tasks` | `-t` | PATH | *required* | Path to task file (JSON or TXT). |
| `--max-workers` | `-w` | INT | `4` | Maximum parallel workers. Overrides settings.ini and task file. |
| `--retries` | `-r` | INT | `0` | Retry count for failed TI executions. Uses exponential backoff. |
| `--result` | `-o` | PATH | *(empty)* | Output CSV path for execution summary. Omit to skip CSV creation. |
| `--settings` | `-s` | PATH | auto | Path to `settings.ini`. Auto-discovered if omitted. |
| `--mode` | `-m` | CHOICE | auto | **Deprecated.** Mode is auto-detected from file content. Ignored. |
| `--exclusive` | `-x` | FLAG | `false` | Enable exclusive mode. Waits for other RushTI sessions to finish. |
| `--force` | `-f` | FLAG | `false` | Bypass exclusive mode checks and run immediately. |
| `--optimize` | | CHOICE | *(none)* | Enable task optimization with a scheduling algorithm: `longest_first` or `shortest_first`. |
| `--no-checkpoint` | | FLAG | `false` | Disable checkpoint saving for this run. |
| `--tm1-instance` | | STR | *(none)* | Read task file from TM1 instead of disk. Requires `--workflow`. |
| `--workflow` | `-W` | STR | *(none)* | Workflow identifier. Defaults to JSON metadata `workflow` field or the taskfile filename stem if omitted. Required when using `--tm1-instance`. |
| `--log-level` | `-L` | CHOICE | `INFO` | Override log level for this run. |

### Examples

```bash
# Basic execution
rushti run --tasks daily-etl.json

# Custom workers and retries
rushti run --tasks daily-etl.json --max-workers 16 --retries 2

# Exclusive mode with result file
rushti run --tasks critical.json --exclusive --result results/critical.csv

# Override workflow name for a file-based run
rushti run --tasks daily-etl.json --workflow DailyETL --max-workers 8

# Read task file from TM1 cube
rushti run --tm1-instance tm1srv01 --workflow DailyETL --max-workers 8

# Optimize with shortest-first scheduling (good for shared-resource TM1 workloads)
rushti run --tasks tasks.json --max-workers 20 --optimize shortest_first

# Optimize with longest-first scheduling (good for independent tasks with varied durations)
rushti run --tasks tasks.json --max-workers 20 --optimize longest_first
```

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All tasks completed successfully |
| `1` | One or more tasks failed |
| `5` | Exclusive mode timeout — could not get access |

---

## rushti resume

Resume an interrupted execution from a checkpoint file. This is the only way to resume -- the `run` command always starts fresh.

### Syntax

```bash
rushti resume --checkpoint FILE [options]
rushti resume --tasks FILE [options]          # auto-finds checkpoint
```

### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--checkpoint` | `-c` | PATH | auto | Path to checkpoint file. Auto-detected from task file if omitted. |
| `--tasks` | `-t` | PATH | *(from checkpoint)* | Path to task file. Defaults to the path stored in the checkpoint. |
| `--resume-from` | | STR | *(none)* | Resume from a specific task ID, overriding checkpoint state. |
| `--max-workers` | `-w` | INT | *(from settings)* | Maximum parallel workers. |
| `--settings` | `-s` | PATH | auto | Path to `settings.ini`. |
| `--force` | `-f` | FLAG | `false` | Force resume even if checkpoint does not match the current task file. |
| `--log-level` | `-L` | CHOICE | `INFO` | Override log level. |

### Examples

```bash
# Resume from a specific checkpoint
rushti resume --checkpoint checkpoints/checkpoint_daily-etl_20260115_103000.json

# Auto-find checkpoint for a task file
rushti resume --tasks daily-etl.json

# Resume from a specific task
rushti resume --tasks daily-etl.json --resume-from transform-data

# List available checkpoints (run with no args)
rushti resume
```

---

## rushti tasks

Task file operations: conversion, expansion, visualization, and validation.

### rushti tasks export

Convert a TXT task file or a TM1-stored task file to JSON format.

```bash
rushti tasks export --tasks input.txt --output output.json
rushti tasks export --tm1-instance tm1srv01 -W DailyETL --output daily.json
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--tasks` | `-t` | PATH | Input task file (TXT or JSON) |
| `--output` | `-o` | PATH | Output JSON file path (*required*) |
| `--tm1-instance` | | STR | Read source from TM1 |
| `--workflow` | `-W` | STR | Workflow in TM1 |
| `--settings` | `-s` | PATH | Path to `settings.ini` |

---

### rushti tasks expand

Expand MDX expressions in task file parameters into concrete tasks. Connects to TM1 to evaluate MDX and generates one task per returned member.

```bash
rushti tasks expand --tasks template.json --output expanded.json
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--tasks` | `-t` | PATH | Input task file with MDX parameters |
| `--output` | `-o` | PATH | Output file path (*required*) |
| `--format` | `-f` | CHOICE | Output format: `json` (default) or `txt` |
| `--tm1-instance` | | STR | TM1 source instance |
| `--workflow` | `-W` | STR | Workflow in TM1 |
| `--settings` | `-s` | PATH | Path to `settings.ini` |

---

### rushti tasks visualize

Generate an interactive HTML DAG visualization from a task file.

```bash
rushti tasks visualize --tasks daily-etl.json --output dag.html
rushti tasks visualize --tasks daily-etl.json --output dag.html --show-parameters
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--tasks` | `-t` | PATH | Input task file |
| `--output` | `-o` | PATH | Output HTML file path (*required*) |
| `--show-parameters` | `-p` | FLAG | Include task parameters in node labels |
| `--tm1-instance` | | STR | TM1 source instance |
| `--workflow` | `-W` | STR | Workflow in TM1 |
| `--settings` | `-s` | PATH | Path to `settings.ini` |

---

### rushti tasks validate

Validate task file structure, dependency graph (cycle detection), and optionally check TM1 connectivity and process existence.

```bash
rushti tasks validate --tasks daily-etl.json
rushti tasks validate --tasks daily-etl.json --skip-tm1-check
rushti tasks validate --tasks daily-etl.json --json > validation.json
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--tasks` | `-t` | PATH | Input task file |
| `--skip-tm1-check` | | FLAG | Skip TM1 connectivity and process validation |
| `--json` | | FLAG | Output results as JSON |
| `--tm1-instance` | | STR | TM1 source instance |
| `--workflow` | `-W` | STR | Workflow in TM1 |
| `--settings` | `-s` | PATH | Path to `settings.ini` |

---

### rushti tasks push

Upload a JSON task file to TM1 as a file in the Applications folder.

```bash
rushti tasks push --tasks daily-etl.json --tm1-instance tm1srv01
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--tasks` | `-t` | PATH | Local JSON task file to push (*required*) |
| `--tm1-instance` | | STR | Source TM1 instance (if loading from TM1) |
| `--target-tm1-instance` | | STR | Target TM1 instance for the push |
| `--settings` | `-s` | PATH | Path to `settings.ini` |

---

## rushti stats

Query and analyze execution statistics from the SQLite stats database.

**Prerequisite:** `[stats] enabled = true` in `settings.ini`.

### rushti stats list

List recent runs or task summaries for a workflow.

```bash
rushti stats list runs --workflow daily-etl
rushti stats list tasks --workflow daily-etl --limit 50
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| *(positional)* | | CHOICE | What to list: `runs` or `tasks` |
| `--workflow` | `-W` | STR | Workflow (*required*) |
| `--limit` | `-n` | INT | Maximum items to show (default: 20) |
| `--settings` | `-s` | PATH | Path to `settings.ini` |

---

### rushti stats export

Export task execution results to a CSV file.

```bash
rushti stats export --workflow daily-etl --output results.csv
rushti stats export --workflow daily-etl --run-id 20260115_103000 --output run.csv
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--workflow` | `-W` | STR | Workflow (*required*) |
| `--run-id` | `-r` | STR | Specific run ID to export (all runs if omitted) |
| `--output` | `-o` | PATH | Output CSV file path (*required*) |
| `--settings` | `-s` | PATH | Path to `settings.ini` |

---

### rushti stats visualize

Generate an interactive HTML dashboard with Gantt charts, success rates, and execution trends. Automatically opens the dashboard in the default browser.

```bash
rushti stats visualize --workflow daily-etl
rushti stats visualize --workflow daily-etl --runs 10 --output dashboard.html
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--workflow` | `-W` | STR | Workflow (*required*) |
| `--runs` | `-n` | INT | Number of recent runs to display (default: 5) |
| `--output` | `-o` | PATH | Output HTML file path (default: `visualizations/rushti_dashboard_<id>.html`) |
| `--settings` | `-s` | PATH | Path to `settings.ini` |

---

### rushti stats analyze

Analyze historical runs, compute EWMA estimates, generate confidence scores, and optionally produce an optimized task file.

```bash
rushti stats analyze --workflow daily-etl --runs 20
rushti stats analyze --workflow daily-etl --tasks daily-etl.json --output optimized.json
rushti stats analyze --workflow daily-etl --report report.json --ewma-alpha 0.5
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--workflow` | `-W` | STR | Workflow (*required*) |
| `--tasks` | `-t` | PATH | Original task file (needed to generate optimized output) |
| `--output` | `-o` | PATH | Output file for optimized task file (JSON) |
| `--report` | | PATH | Output file for analysis report (JSON) |
| `--runs` | `-n` | INT | Number of recent runs to analyze (default: 10) |
| `--ewma-alpha` | | FLOAT | EWMA smoothing factor 0--1 (default: 0.3). Higher = more weight on recent runs. |
| `--settings` | `-s` | PATH | Path to `settings.ini` |

---

## rushti build

Create TM1 dimensions and the rushti cube for execution logging and TM1 integration.

### Syntax

```bash
rushti build --tm1-instance INSTANCE [options]
```

### Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--tm1-instance` | | STR | TM1 instance name from `config.ini` (*required*) |
| `--force` | `-f` | FLAG | Delete and recreate existing objects |
| `--settings` | `-s` | PATH | Path to `settings.ini` |
| `--log-level` | `-L` | CHOICE | Override log level |

### Examples

```bash
# Create logging objects
rushti build --tm1-instance tm1srv01

# Force recreate
rushti build --tm1-instance tm1srv01 --force
```

### Objects Created

| Object | Type | Description |
|--------|------|-------------|
| `rushti_workflow` | Dimension | Workflow identifiers |
| `rushti_task_id` | Dimension | Task sequence elements (1--5000) |
| `rushti_run_id` | Dimension | Run timestamps + `Input` element |
| `rushti_measure` | Dimension | Log field measures |
| `rushti` | Cube | Task definitions and execution results |

---

## rushti db

Database administration commands for the SQLite stats database.

!!! note "db vs stats"
    Use `rushti db` for administrative operations (clear data, vacuum, list workflows). Use `rushti stats` for querying and analyzing execution data.

### rushti db list workflows

Show all workflows tracked in the database with run counts and last execution time.

```bash
rushti db list workflows
```

---

### rushti db clear

Delete data from the stats database.

```bash
# Delete all data for a workflow
rushti db clear --workflow old-workflow

# Delete a specific run
rushti db clear --run-id 20260115_103000

# Delete data before a date
rushti db clear --before 2025-01-01

# Delete everything (requires confirmation)
rushti db clear --all

# Preview deletions without executing
rushti db clear --workflow old-workflow --dry-run
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--workflow` | `-W` | STR | Delete all data for this workflow |
| `--run-id` | `-r` | STR | Delete data for a specific run |
| `--before` | | DATE | Delete data before this date (YYYY-MM-DD) |
| `--all` | | FLAG | Delete all data (requires confirmation) |
| `--dry-run` | | FLAG | Preview changes without executing |

---

### rushti db show

Display detailed information about a run or task execution history.

```bash
# Show run details with task-level breakdown
rushti db show run --run-id 20260115_103000

# Show execution history for a task signature
rushti db show task --signature "tm1srv01|Finance.Extract.Data|pYear=2026"
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--run-id` | `-r` | STR | Run ID to show details for |
| `--signature` | | STR | Task signature to show history for |
| `--limit` | `-n` | INT | Maximum items (default: 20) |

---

### rushti db vacuum

Compact the SQLite database file by reclaiming unused space.

```bash
rushti db vacuum
```

Reports the database size before and after compaction.

---

## Common Workflows

### Development

```bash
# 1. Validate the task file
rushti tasks validate --tasks workflow.json --skip-tm1-check

# 2. Visualize dependencies
rushti tasks visualize --tasks workflow.json --output dag.html

# 3. Test with few workers and debug logging
rushti run --tasks workflow.json --max-workers 2 --log-level DEBUG
```

### Production

```bash
# Execute with exclusive mode and checkpointing
rushti run --tasks production-etl.json --max-workers 20 --exclusive

# If interrupted, resume
rushti resume --tasks production-etl.json
```

### Optimization

```bash
# 1. Run several times to collect history
rushti run --tasks workflow.json --max-workers 8

# 2. Enable optimization on subsequent runs
rushti run --tasks workflow.json --max-workers 8 --optimize shortest_first

# 3. Analyze and generate optimized task file
rushti stats analyze --workflow workflow --tasks workflow.json --output optimized.json --runs 20

# 4. Validate and compare
rushti tasks validate --tasks optimized.json --skip-tm1-check
```

---

## Next Steps

- **[Settings Reference](settings-reference.md)** -- All `settings.ini` options
- **[Advanced Task Files](advanced-task-files.md)** -- Stages, timeouts, expandable parameters
- **[Performance Tuning](performance-tuning.md)** -- Optimizing execution throughput
