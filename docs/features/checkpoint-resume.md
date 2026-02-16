# Checkpoint & Resume

RushTI can save progress during long-running workflows. If something fails -- a TM1 crash, network timeout, or TI process error -- you can resume from where it left off instead of starting over.

---

## Working Example

### A Typical Scenario

You are running a 500-task month-end close that takes 2 hours. After 90 minutes and 450 completed tasks, the TM1 server restarts unexpectedly.

**Without checkpoint:** You re-run all 500 tasks from scratch. Another 2 hours lost.

**With checkpoint:** You resume from where it stopped. Only the remaining 50 tasks need to run.

```bash
# Original run (interrupted after 450 tasks)
rushti run --tasks monthly-close.json --max-workers 8

# Resume -- only the remaining tasks execute
rushti resume --tasks monthly-close.json
```

---

## How to Enable

Add the `[resume]` section to `config/settings.ini`:

```ini
[resume]
enabled = true
checkpoint_interval = 60
checkpoint_dir = ./checkpoints
```

| Setting | Default | Range | Description |
|---------|---------|-------|-------------|
| `enabled` | `false` | -- | Turn on checkpoint saving |
| `checkpoint_interval` | `60` | 10-600 seconds | How often to save progress |
| `checkpoint_dir` | `./checkpoints` | -- | Directory for checkpoint files |

That is all the setup required. Once enabled, RushTI automatically saves checkpoints during every run.

---

## How It Works

### Saving Checkpoints

When checkpoints are enabled, RushTI saves a checkpoint file every 60 seconds (configurable). The checkpoint records:

- Which tasks **succeeded** (with timing data)
- Which tasks **failed** (with error details)
- Which tasks were **in progress** when the checkpoint was saved
- Which tasks are still **pending** (not yet started)

Checkpoint files are saved as JSON in the checkpoint directory:

```
checkpoints/
  monthly-close_checkpoint.json
  daily-refresh_checkpoint.json
```

### Resuming

When you run `rushti resume`, RushTI:

1. Loads the checkpoint file for the specified workflow
2. Validates it against the current task file (warns you if the file changed)
3. Marks all previously succeeded tasks as complete (skips them)
4. Runs only the failed, in-progress, and pending tasks
5. Continues saving new checkpoints as it goes

```bash
# Resume from the most recent checkpoint
rushti resume --tasks monthly-close.json

# Resume from a specific checkpoint file
rushti resume --checkpoint checkpoints/monthly-close_20260209_143022.json
```

### What Gets Resumed

| Task Status at Checkpoint | What Happens on Resume |
|--------------------------|----------------------|
| Succeeded | Skipped (already done) |
| Failed | Re-run |
| Pending | Run normally |
| In progress (`safe_retry: true`) | Re-run from the beginning |
| In progress (`safe_retry: false`) | Requires manual decision |

!!! info "The `safe_retry` Flag"
    Mark tasks as `safe_retry: true` when they are **idempotent** -- running them twice produces the same result. Examples: clearing and rebuilding a cube view, mantain subsets, exporting a report. Tasks that append data or send emails should stay `safe_retry: false` (the default).

---

## Checkpoint File Contents

A checkpoint file is a simple JSON document. Here is a shortened example:

```json
{
  "version": "1.0",
  "workflow": "monthly-close",
  "taskfile_path": "/rushti/tasks/monthly-close.json",
  "run_started": "2026-02-09T14:00:00",
  "checkpoint_created": "2026-02-09T15:30:00",
  "total_tasks": 500,
  "summary": {
    "completed": 450,
    "in_progress": 2,
    "pending": 48,
    "failed": 0,
    "progress_percentage": 90.0
  },
  "completed_tasks": {
    "1": { "success": true, "duration_seconds": 12.5 },
    "2": { "success": true, "duration_seconds": 15.2 }
  },
  "in_progress_tasks": ["7", "8"],
  "pending_tasks": ["11", "12"]
}
```

You can inspect a checkpoint any time to see progress:

```bash
# Quick summary (requires jq)
cat checkpoints/monthly-close_checkpoint.json | jq '.summary'
```

---

## Best Practices

### Enable for Long-Running Workflows

Any workflow that takes more than 5 minutes is a good candidate for checkpoints. The overhead is minimal (a small JSON file written to disk every 60 seconds).

### Tune the Checkpoint Interval

| Workflow Type | Recommended Interval | Reason |
|---------------|---------------------|--------|
| Many short tasks (< 10s each) | `30` seconds | Capture fast progress |
| Mix of short and long tasks | `60` seconds (default) | Good balance |
| Few long tasks (> 5 min each) | `120-300` seconds | Less I/O, tasks are slow anyway |

```ini
[resume]
checkpoint_interval = 30    # For workflows with many quick tasks
```

### Mark Idempotent Tasks

Set `safe_retry: true` on tasks that can safely re-run:

=== "JSON"

    ```json
    {
      "id": "5",
      "instance": "tm1-finance",
      "process": "System.RebuildAggregations",
      "safe_retry": true
    }
    ```

=== "TXT"

    ```text
    id="5" safe_retry="true" instance="tm1-finance" process="System.RebuildAggregations"
    ```

**Good candidates for `safe_retry: true`:**

- Clear-and-rebuild processes (dimension updates, view refreshes)
- Read-only exports and report generation
- Cache refresh and metadata operations

**Keep `safe_retry: false` (default) for:**

- Incremental data loads (appending transactions)
- Processes that send emails or trigger external systems
- Anything that creates new records with auto-generated IDs

### Use `--force` to Start Fresh

If a checkpoint exists but you want to ignore it and start over:

```bash
rushti run --tasks monthly-close.json --max-workers 8 --force
```

The `--force` flag tells RushTI to discard any existing checkpoint and begin a full run.

---

## Troubleshooting

### "Checkpoint not found"

```
ERROR: Checkpoint not found for workflow: monthly-close
```

The checkpoint may have been cleaned up after a successful run (this is the default behavior). Check your checkpoint directory:

```bash
ls checkpoints/
```

!!! tip "Checkpoint Cleanup"
    Checkpoint files remain in the checkpoint directory after a run completes. Clean them up manually or include cleanup in your scheduling scripts.

### "Task file has been modified"

```
WARNING: Task file has been modified since checkpoint
```

You changed the task file after the checkpoint was created. RushTI warns you because the checkpoint might not match the current tasks. Options:

- **Force resume** if your changes are compatible (e.g., you only added a new task):
  ```bash
  rushti resume --tasks monthly-close.json --force
  ```
- **Start fresh** if your changes are significant:
  ```bash
  rushti run --tasks monthly-close.json --max-workers 8
  ```

### "Cannot automatically resume -- non-safe-retry tasks"

```
ERROR: 2 tasks were in-progress with safe_retry=false
```

These tasks were running when the interruption happened, and RushTI cannot guarantee they are safe to re-run. You have two options:

1. **Manually verify** the tasks completed or can be re-run, then force resume
2. **Mark them as `safe_retry: true`** in the task file if they are actually idempotent

---

## Configuration Summary

All checkpoint settings in one place:

```ini
[resume]
enabled = true                     # Save checkpoints during execution
checkpoint_interval = 60           # Seconds between checkpoint saves
checkpoint_dir = ./checkpoints     # Where to store checkpoint files
auto_resume = false                # Automatically resume from last checkpoint on restart
```

---

## Customize Further

- **[Settings Reference](../advanced/settings-reference.md)** -- Complete `[resume]` settings documentation
- **[CLI Reference](../advanced/cli-reference.md)** -- Full CLI options for the resume command
- **[DAG Execution](dag-execution.md)** -- How task scheduling and failure handling interact with checkpoints
- **[Exclusive Mode](exclusive-mode.md)** -- Prevent concurrent executions that could conflict with a resumed run
