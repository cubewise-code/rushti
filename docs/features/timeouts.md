# Run Phase Time Limits

RushTI is usually started by a TI process through `ExecuteCommand` with wait = 1, so the calling TI waits for as long as RushTI runs. If RushTI hangs (a login blocked by a lock, a task deadlocked with the calling TI, a results push into a locked cube), the calling TI hangs with it.

Run phase time limits stop that. Give a run phase a limit, and when the phase runs past it RushTI writes a `TimedOut` result, logs out of TM1 and exits, so the calling TI can carry on and react.

Time limits are **off by default**. Nothing changes until you set one.

---

## Working Example

```bash
rushti run --tm1-instance tm1srv01 --workflow DailyETL \
  --result results/DailyETL.csv \
  --timeout-task-retrieval 60 --timeout-connection 60 --timeout-task-execution 7200
```

If logging in to `tm1srv01` blocks for more than 60 seconds, RushTI logs:

```
ERROR - Run phase 'task_retrieval' went past its time limit of 60s. RushTI is terminating and logging out of TM1.
```

and writes `results/DailyETL.csv`:

```
PID|Process Runs|Process Fails|Start|End|Runtime|Overall Success|Status
14820|0|0|2026-10-19 06:00:00.112|2026-10-19 06:01:00.118|0:01:00.006|False|TimedOut
```

then exits with code 1.

---

## Run Phases

Every run goes through these phases, in this order. Each has its own limit.

| Phase | Starts | Ends | Used when |
|-------|--------|------|-----------|
| `task_retrieval` | Connecting to the source instance | The taskfile has been read from the `rushti` cube | Only for TM1 sources (`--tm1-instance`) |
| `connection` | Connecting to the TM1 instances the tasks run on | The first task starts | Every run |
| `task_execution` | The first task starts | The last task finishes | Every run |
| `push_results` | Connecting to the results target | `}rushti.load.results` has finished | Only when `push_results = true` |

A few details:

- **`connection`** includes everything between logging in and the first task starting: expanding MDX parameters and checking that the processes exist. It leaves out the [exclusive-mode](exclusive-mode.md) wait, which has its own limit (`[exclusive_mode] timeout`); the clock pauses while RushTI waits for exclusive access.
- **`task_retrieval`** doesn't apply to taskfiles read from disk. Setting it for a `--tasks` run has no effect.
- Only elapsed time counts. RushTI never reads TM1 threads, sessions or locks to decide a run is stuck, so there's no extra load on the server.

---

## Configuration

Limits are in seconds. `0` (or an empty value in `settings.ini`) means no limit. Negative or non-integer values are an error.

=== "settings.ini (All Runs)"

    ```ini
    [timeouts]
    task_retrieval = 60
    connection = 60
    task_execution = 3600
    push_results = 120
    ```

    Best for: a safety net on every run in an environment.

=== "Task File Settings"

    ```json
    {
      "version": "2.0",
      "settings": {
        "timeouts": {
          "connection": 60,
          "task_execution": 7200
        }
      },
      "tasks": [...]
    }
    ```

    Best for: workflows whose normal duration you know. Phases you leave out keep the `settings.ini` value. Unknown phase names are an error.

=== "CLI Flags"

    ```bash
    rushti run --tasks month-end.json \
      --timeout-connection 60 \
      --timeout-task-execution 14400 \
      --timeout-push-results 120
    ```

    Best for: the TI process that calls RushTI, which knows how long it can afford to wait. The flags are `--timeout-task-retrieval`, `--timeout-connection`, `--timeout-task-execution` and `--timeout-push-results`.

Precedence is the usual one: CLI > taskfile `settings.timeouts` > `settings.ini [timeouts]` > no limit. A `0` at a higher tier removes a limit set at a lower one, e.g. `--timeout-task-execution 0` for a one-off long run.

!!! note "TM1 cube taskfiles"
    A taskfile read from the `rushti` cube has no settings block, so its limits come from the CLI and `settings.ini`.

---

## What Happens on a Hang

When a phase goes past its limit, RushTI:

1. **Logs** which phase limit was hit and its value, at error level.
2. **Writes the result file** with `Status` = `TimedOut` and `Overall Success` = `False`. `Process Runs` and `Process Fails` count the tasks that finished before the limit.
3. **Cleans up locally**, without calling TM1:
    - saves the [checkpoint](checkpoint-resume.md) and keeps it, so you can `rushti resume`;
    - flushes the execution logs;
    - writes the stats to the local SQLite database. With the DynamoDB backend, stats for the run are not written.
4. **Logs out of TM1**, in the background, from every session it opened. Preserved connections (`connection_file` in `config.ini`) are left alone, unless the run is in exclusive mode, as with [early session release](exclusive-mode.md#early-session-release). RushTI waits at most about 5 seconds for the logouts and logs a warning naming any instance whose logout didn't finish.
5. **Exits** with code 1.

It skips the normal end of a run: no results push to TM1 and no stats written to DynamoDB.

!!! warning "Running TI processes are not stopped"
    RushTI doesn't cancel tasks, and the logout doesn't stop them either. A TI process that was mid-way through when the limit was hit carries on to completion on the server and commits, after RushTI has exited (on both TM1 v11 and v12). RushTI stops waiting for it; it doesn't know how it ended. Trace the real outcome of those processes on the server (the TM1 message log or the process error logs).

### Resuming After a Hang

A `task_execution` timeout leaves the checkpoint in place. The tasks that were still running show as in progress, even though they may since have completed on the server. Once you know why the run hung and how those tasks really ended, resume it:

```bash
rushti resume --tasks month-end.json
```

In-progress tasks are handled by their `safe_retry` setting, as described in [Checkpoint & Resume](checkpoint-resume.md#what-gets-resumed): a `safe_retry: true` task is re-run even if it did complete, so only mark tasks that are safe to run twice. `rushti resume` accepts the same `--timeout-*` flags.

---

## How It Differs from Other Timeouts

| | Run phase limits | Per-task `timeout` | Exclusive-mode `timeout` |
|---|---|---|---|
| **Limits** | A whole run phase | One task | The wait for exclusive access |
| **Set in** | `[timeouts]`, `settings.timeouts`, `--timeout-*` | The task in the taskfile | `[exclusive_mode] timeout` |
| **Default** | No limit | No limit | 600 seconds |
| **When hit** | Run ends, TM1 sessions logged out; running TI processes carry on | Task marked failed, run carries on | Run ends before any task starts |
| **Result file `Status`** | `TimedOut` | `Failed` (if no other error) | `Aborted` |

Use per-task `timeout` for a task that's known to run long sometimes, and run phase limits as the safety net for the run as a whole.

!!! warning "TM1 v12: per-task `cancel_at_timeout` doesn't cancel the process"
    On TM1 v12 and later a task's TI process is **not** cancelled when its `timeout` expires. RushTI cancels through TM1py, and TM1py's `ThreadService` blocks itself on servers v12 and later; its `MonitoringService` thread methods just call `ThreadService`. So RushTI stops waiting while the process keeps running on the server.

---

## Choosing Limits

- **`task_retrieval` and `connection`**: these normally take seconds. A limit of 60 seconds catches a blocked login without risking a false alarm. Raise `connection` if your taskfile expands large MDX parameter sets.
- **`task_execution`**: set it well above your longest normal run. `rushti stats list runs --workflow <workflow>` shows past durations.
- **`push_results`**: covers the upload and `}rushti.load.results`. A couple of minutes is plenty for most workflows.

A timeout always calls for a look at what was blocking. The log says which phase ran over, and TM1's thread monitor shows what the sessions were waiting on.

---

## Result File `Status`

Every run writes a `Status` column at the end of the `--result` file:

| Status | Meaning |
|--------|---------|
| `Success` | The run finished and every task succeeded |
| `Failed` | The run finished and at least one task failed |
| `Aborted` | The run stopped on an error: invalid taskfile, failed taskfile read, missing `--config`, a fatal execution error, or the exclusive-mode wait running out |
| `TimedOut` | A run phase went past its time limit |

!!! warning "New column"
    `Status` is new. If a TI process reads the result file with a fixed set of variables, check that it still works with the extra trailing column.

---

## Customize Further

- **[Settings Reference](../advanced/settings-reference.md)** -- The `[timeouts]` section
- **[CLI Reference](../advanced/cli-reference.md)** -- The `--timeout-*` flags
- **[Checkpoint & Resume](checkpoint-resume.md)** -- Resuming after a `task_execution` timeout
- **[TM1 Integration](tm1-integration.md)** -- Calling RushTI from a TI process
