# Advanced Task Files

Beyond basic tasks and dependencies, RushTI JSON task files support stages, timeouts, expandable parameters, and fine-grained control over dependency behavior. This page covers the features that give you precise control over complex TM1 workflows.

---

## Stages

Stages group tasks into logical pipeline phases. All tasks within a stage run in parallel (up to `max_workers` or the stage's `stage_workers` limit), and stages execute sequentially -- all tasks in stage N must complete before any task in stage N+1 begins.

### How Stages Work

Assign a `stage` label to each task. Define the execution order with `stage_order` in the `settings` section. Tasks without explicit predecessors inherit implicit dependencies: every task in a stage depends on all tasks in the previous stage.

```json
{
  "version": "2.0",
  "settings": {
    "max_workers": 8,
    "stage_order": ["extract", "transform", "consolidate", "report", "notify"]
  },
  "tasks": [
    { "id": "1", "stage": "extract",      "instance": "tm1-finance",   "process": "Close.Extract.Regional",          "parameters": { "pRegion": "NorthAmerica" } },
    { "id": "2", "stage": "extract",      "instance": "tm1-finance",   "process": "Close.Extract.Regional",          "parameters": { "pRegion": "Europe" } },
    { "id": "3", "stage": "transform",    "instance": "tm1-finance",   "process": "Close.Transform.Currency",        "parameters": { "pRegion": "NorthAmerica" } },
    { "id": "4", "stage": "transform",    "instance": "tm1-finance",   "process": "Close.Transform.Currency",        "parameters": { "pRegion": "Europe" } },
    { "id": "5", "stage": "consolidate",  "instance": "tm1-finance",   "process": "Close.Consolidate.Global" },
    { "id": "6", "stage": "report",       "instance": "tm1-reporting", "process": "Report.Generate.ManagementPack" },
    { "id": "7", "stage": "notify",       "instance": "tm1-finance",   "process": "Close.Notify.Completion" }
  ]
}
```

In this example, tasks `1` and `2` run in parallel. Once both finish, tasks `3` and `4` start in parallel. Then `5`, then `6`, then `7`.

### Combining Stages with Explicit Predecessors

When you define both `stage` and `predecessors` on a task, the predecessors provide fine-grained control within the stage structure. This is the most common pattern for real workflows:

```json
{
  "id": "5",
  "stage": "transform",
  "instance": "tm1-finance",
  "process": "Close.Transform.Currency",
  "parameters": { "pRegion": "NorthAmerica", "pTargetCurrency": "USD" },
  "predecessors": ["1", "4"],
  "require_predecessor_success": true
}
```

Here, task `5` is in the transform stage but only starts after tasks `1` and `4` specifically complete (not all extract-stage tasks).

### Limiting Concurrency per Stage

Use `stage_workers` in the settings section to cap parallelism for resource-intensive stages:

```json
{
  "settings": {
    "max_workers": 16,
    "stage_order": ["extract", "transform", "load"],
    "stage_workers": {
      "extract": 16,
      "transform": 8,
      "load": 4
    }
  }
}
```

This runs up to 16 extracts in parallel, but limits transforms to 8 concurrent workers and loads to 4, preventing TM1 write contention.

!!! note "Global max_workers Cap"
    The global `max_workers` setting defines the TM1 session connection pool size and acts as an absolute ceiling. If a `stage_workers` value exceeds `max_workers`, the global limit still applies and a warning is logged. For example, with `max_workers: 8` and `stage_workers: {"extract": 16}`, at most 8 extract tasks run concurrently.

### Finance Close Example

The [`finance-close.json`](../samples/finance-close.json) sample demonstrates a full five-stage pipeline: extract, transform, consolidate, report, and notify. It uses stages with explicit predecessors and per-task timeouts.

---

## Timeouts

Per-task timeouts prevent runaway TI processes from blocking the entire workflow.

### Task-Level Timeout

Set the `timeout` field to the maximum allowed execution time in seconds:

```json
{
  "id": "12",
  "instance": "tm1-reporting",
  "process": "Report.Generate.Regulatory",
  "parameters": { "pFormat": "XBRL" },
  "timeout": 600
}
```

If the task exceeds 600 seconds, RushTI marks it as failed and moves on to downstream tasks (subject to their `require_predecessor_success` setting).

### Cancel at Timeout

By default, a timed-out task's TI process continues running on the TM1 server -- only the RushTI tracking moves on. Set `cancel_at_timeout` to `true` to actively cancel the TI process when the timeout expires:

```json
{
  "id": "12",
  "instance": "tm1-reporting",
  "process": "Report.Generate.Regulatory",
  "timeout": 600,
  "cancel_at_timeout": true
}
```

!!! warning "When to Use cancel_at_timeout"
    Only use `cancel_at_timeout` for non-critical tasks or tasks that are safe to interrupt. Cancelling a TI process mid-execution can leave data in an inconsistent state if the process modifies cube data without proper transaction handling.

### Timeout Behavior Summary

| `timeout` | `cancel_at_timeout` | Behavior on Timeout |
|-----------|---------------------|---------------------|
| Not set | N/A | Task runs indefinitely |
| Set | `false` (default) | Task marked as failed; TI process continues on server |
| Set | `true` | Task marked as failed; TI process cancelled on server |

---

## Expandable Parameters

Expandable parameters let you define a single task template that expands into multiple concrete tasks at runtime. RushTI connects to TM1, evaluates the MDX expression, and creates one task per returned member.

### Syntax

Mark a parameter as expandable by appending `*` to the parameter name and wrapping the MDX expression with `*{...}`:

```json
{
  "parameters": {
    "pRegion*": "*{TM1FILTERBYLEVEL({TM1SUBSETALL([Region].[Region])}, 0)}"
  }
}
```

The key markers:

- **`pRegion*`** -- The trailing `*` on the parameter name marks it as expandable.
- **`*{...}`** -- The `*{` prefix and `}` suffix wrap the MDX expression.

### How Expansion Works

Given a `[Region]` dimension with leaf members `NorthAmerica`, `Europe`, and `AsiaPacific`, the single task:

```json
{
  "id": "1",
  "instance": "tm1-finance",
  "process": "Close.Extract.Regional",
  "parameters": {
    "pRegion*": "*{TM1FILTERBYLEVEL({TM1SUBSETALL([Region].[Region])}, 0)}",
    "pPeriod": "Current"
  }
}
```

Expands into three tasks:

```json
{ "id": "1_NorthAmerica", "process": "Close.Extract.Regional", "parameters": { "pRegion": "NorthAmerica", "pPeriod": "Current" } },
{ "id": "1_Europe",       "process": "Close.Extract.Regional", "parameters": { "pRegion": "Europe",       "pPeriod": "Current" } },
{ "id": "1_AsiaPacific",  "process": "Close.Extract.Regional", "parameters": { "pRegion": "AsiaPacific",  "pPeriod": "Current" } }
```

Each expanded task ID is the original ID with the member name appended after an underscore.

### Expanding Before Execution

Use `rushti tasks expand` to materialize the expanded tasks into a new file before running:

```bash
# Expand MDX parameters (connects to TM1 to evaluate expressions)
rushti tasks expand --tasks finance-close-expandable.json --output finance-close-expanded.json

# Run the expanded file
rushti run --tasks finance-close-expanded.json --max-workers 8
```

This two-step approach is useful when you want to inspect or modify the expanded tasks before execution, or when you need to run the same expanded set multiple times without re-querying TM1.

### Expansion During Execution

When you run a task file directly without pre-expanding, RushTI expands the parameters automatically at startup before building the DAG. The expanded tasks are never written to disk in this case.

### Predecessor Handling

When an expanded task is referenced as a predecessor, RushTI automatically creates dependencies on all expanded variants:

```json
{
  "id": "4",
  "process": "Close.Consolidate.Global",
  "predecessors": ["1"]
}
```

This task waits for `1_NorthAmerica`, `1_Europe`, and `1_AsiaPacific` to all complete before starting.

### Expandable Parameters Example

The [`finance-close-expandable.json`](../samples/finance-close-expandable.json) sample shows the expandable version of the finance close workflow, where regions are dynamically derived from TM1 instead of being hardcoded.

---

## Conditional Execution

### require_predecessor_success

Controls whether a task runs only when all predecessors succeeded or whenever predecessors complete (regardless of status).

| Value | Behavior |
|-------|----------|
| `false` (default) | Task runs when all predecessors have completed, regardless of their success or failure. |
| `true` | Task runs only if **every** predecessor completed successfully. If any predecessor failed, this task is skipped. |

**When to use `true`:**

- Data transformation tasks that depend on successful extraction
- Consolidation tasks that need valid input data
- Any task where running on bad data would produce incorrect results

```json
{
  "id": "5",
  "process": "Close.Transform.Currency",
  "predecessors": ["1", "4"],
  "require_predecessor_success": true
}
```

**When to keep `false` (default):**

- Cleanup or notification tasks that should run regardless of upstream failures
- Error-handling tasks that need to execute after a failure
- Final reporting tasks that should report on whatever completed

```json
{
  "id": "13",
  "process": "Close.Notify.Completion",
  "predecessors": ["11", "12"],
  "require_predecessor_success": false
}
```

### succeed_on_minor_errors

When set to `true`, TI processes that complete with minor errors (ProcessCompletedWithMessages) are treated as successful instead of failed.

```json
{
  "id": "1",
  "process": "Extract.Sales.Data",
  "succeed_on_minor_errors": true
}
```

### safe_retry

Marks a task as idempotent -- safe to re-execute during resume without side effects. Used by the checkpoint/resume system to determine which in-progress tasks can be automatically retried.

```json
{
  "id": "2",
  "process": "Export.Sales.Data",
  "safe_retry": true
}
```

---

## Complete Example

The following task file combines stages, timeouts, expandable parameters, and conditional execution for a monthly financial close:

```json
{
  "version": "2.0",
  "metadata": {
    "workflow": "finance-close-advanced",
    "name": "Monthly Financial Close (Advanced)",
    "description": "Full close process with dynamic regions, stage-limited concurrency, timeouts, and conditional execution",
    "author": "TM1 Admin"
  },
  "settings": {
    "max_workers": 12,
    "retries": 2,
    "exclusive": true,
    "stage_order": ["extract", "transform", "consolidate", "report", "notify"],
    "stage_workers": {
      "extract": 12,
      "transform": 8,
      "consolidate": 4,
      "report": 4,
      "notify": 2
    }
  },
  "tasks": [
    {
      "id": "1",
      "instance": "tm1-finance",
      "process": "Close.Extract.Regional",
      "parameters": {
        "pRegion*": "*{TM1FILTERBYLEVEL({TM1SUBSETALL([Region].[Region])}, 0)}",
        "pPeriod": "Current"
      },
      "stage": "extract",
      "safe_retry": true,
      "timeout": 120
    },
    {
      "id": "2",
      "instance": "tm1-finance",
      "process": "Close.Extract.ExchangeRates",
      "parameters": { "pSource": "Bloomberg" },
      "stage": "extract",
      "safe_retry": true,
      "timeout": 60
    },
    {
      "id": "3",
      "instance": "tm1-finance",
      "process": "Close.Transform.Currency",
      "parameters": {
        "pRegion*": "*{TM1FILTERBYLEVEL({TM1SUBSETALL([Region].[Region])}, 0)}",
        "pTargetCurrency": "USD"
      },
      "predecessors": ["1", "2"],
      "stage": "transform",
      "require_predecessor_success": true
    },
    {
      "id": "4",
      "instance": "tm1-finance",
      "process": "Close.Consolidate.Global",
      "parameters": { "pVersion": "Actual" },
      "predecessors": ["3"],
      "stage": "consolidate",
      "require_predecessor_success": true
    },
    {
      "id": "5",
      "instance": "tm1-finance",
      "process": "Close.Consolidate.Intercompany",
      "parameters": { "pEliminate": "Yes" },
      "predecessors": ["3"],
      "stage": "consolidate"
    },
    {
      "id": "6",
      "instance": "tm1-finance",
      "process": "Close.Validate.Balances",
      "predecessors": ["4", "5"],
      "stage": "consolidate",
      "require_predecessor_success": true
    },
    {
      "id": "7",
      "instance": "tm1-reporting",
      "process": "Report.Generate.ManagementPack",
      "parameters": { "pFormat": "PDF", "pDistribution": "Email" },
      "predecessors": ["6"],
      "stage": "report",
      "timeout": 300
    },
    {
      "id": "8",
      "instance": "tm1-reporting",
      "process": "Report.Generate.Regulatory",
      "parameters": { "pFormat": "XBRL" },
      "predecessors": ["6"],
      "stage": "report",
      "timeout": 600,
      "cancel_at_timeout": true
    },
    {
      "id": "9",
      "instance": "tm1-finance",
      "process": "Close.Notify.Completion",
      "parameters": { "pRecipients": "finance-team@company.com" },
      "predecessors": ["7", "8"],
      "stage": "notify",
      "require_predecessor_success": false
    }
  ]
}
```

### What This File Does

1. **Extract stage** (up to 12 workers): Dynamically expands regions from TM1 and extracts data for each, plus fetches exchange rates. All extract tasks have timeouts and are safe to retry.
2. **Transform stage** (up to 8 workers): Currency conversion for each region. Only runs if extracts succeeded (`require_predecessor_success: true`).
3. **Consolidate stage** (up to 4 workers): Global consolidation, intercompany elimination, and balance validation. Validation requires both consolidation tasks to succeed.
4. **Report stage** (up to 4 workers): Generates management and regulatory reports. Regulatory report will be cancelled if it exceeds 10 minutes.
5. **Notify stage** (up to 2 workers): Sends completion notification. Runs regardless of whether reports succeeded, ensuring the team is always notified.

---

## Task Property Quick Reference

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `id` | string | *required* | Unique task identifier |
| `instance` | string | *required* | TM1 instance name from `config.ini` |
| `process` | string | *required* | TI process name |
| `parameters` | object | `{}` | Process parameters as name-value pairs |
| `predecessors` | array | `[]` | Task IDs that must complete first |
| `stage` | string | `null` | Execution stage label |
| `timeout` | integer | `null` | Max execution time in seconds |
| `cancel_at_timeout` | boolean | `false` | Cancel TI process on timeout |
| `require_predecessor_success` | boolean | `false` | Only run if all predecessors succeeded |
| `safe_retry` | boolean | `false` | Task is idempotent, safe to retry on resume |
| `succeed_on_minor_errors` | boolean | `false` | Treat ProcessCompletedWithMessages as success |

---

## Next Steps

- **[Settings Reference](settings-reference.md)** -- Task file settings section options
- **[Performance Tuning](performance-tuning.md)** -- Optimizing parallel execution
- **[CLI Reference](cli-reference.md)** -- `rushti tasks expand` and `rushti tasks validate` commands
