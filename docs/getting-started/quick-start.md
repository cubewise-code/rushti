# Quick Start

Get RushTI running in 10 minutes. By the end you'll have a parallel workflow executing TI processes on your TM1 server.

!!! note "Prerequisites"
    Make sure you've completed [Installation](installation.md) — RushTI is installed and `config/config.ini` is configured with your TM1 connection.

## Step 1: Create a Task File (Normal Mode)

The quickest way to get started is with a plain TXT file. Save this as `my-tasks.txt`:

```text
instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=2
instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=5
instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=3
wait
instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=4
instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=2
wait
instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=3
```

!!! tip
    `}bedrock.server.wait` ships with every [Bedrock](https://github.com/cubewise-code/bedrock) installation — it just waits for the specified number of seconds. Perfect for testing without creating any custom TI processes.

**What this does:** Each line is a task. Tasks between `wait` lines run in parallel. The first three tasks execute simultaneously (~5s instead of ~10s sequential), then `wait` pauses until all three finish, the next two run in parallel, another `wait`, then the final task.

## Step 2: Run It

```bash
rushti run --tasks my-tasks.txt --max-workers 4
```

```
RushTI starts. Parameters: ['my-tasks.txt', '--max-workers', '4']
Loaded 6 tasks from TXT task file (classic mode)

Executing process: '}bedrock.server.wait' on instance: 'tm1srv01'
Executing process: '}bedrock.server.wait' on instance: 'tm1srv01'
Executing process: '}bedrock.server.wait' on instance: 'tm1srv01'
Execution successful: '}bedrock.server.wait' (pWaitSec=2) completed in 2.1s
Execution successful: '}bedrock.server.wait' (pWaitSec=3) completed in 3.1s
Execution successful: '}bedrock.server.wait' (pWaitSec=5) completed in 5.1s
Executing process: '}bedrock.server.wait' on instance: 'tm1srv01'
Executing process: '}bedrock.server.wait' on instance: 'tm1srv01'
...
RushTI ends. 0 fails out of 6 executions. Elapsed time: 12.3s
```

!!! success
    The first three tasks ran in parallel (~5s) instead of sequentially (~10s). The `wait` keyword controlled the flow between groups.

## Step 3: Add Dependencies (Optimized Mode)

The `wait` keyword is simple but coarse — it blocks *all* tasks until the entire group finishes. With **optimized mode**, you can say exactly which tasks depend on which:

Save this as `my-tasks-opt.txt`:

```text
id="1" predecessors="" instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=2
id="2" predecessors="" instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=5
id="3" predecessors="" instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=3
id="4" predecessors="2" instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=4
id="5" predecessors="1,3,4" instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=2
id="6" predecessors="5" instance="tm1srv01" process="}bedrock.server.wait" pWaitSec=3
```

Each task has an `id` and lists its `predecessors` — the tasks that must finish before it can start. Task `4` only waits for task `2`, not all of the first group. Task `5` waits for `1`, `3`, and `4`.

```bash
rushti run --tasks my-tasks-opt.txt --max-workers 4
```

```
Timeline (optimized):
  Task 1 (2s):  [==]
  Task 2 (5s):  [=====]
  Task 3 (3s):  [===]
  Task 4 (4s):       [====]          ← starts when task 2 done
  Task 5 (2s):              [==]     ← starts when 1, 3, 4 done
  Task 6 (3s):                [===]
                Total: ~14s
```

!!! info "How does RushTI read task files?"
    Internally, RushTI converts every TXT task file into JSON before executing. JSON is the native format — TXT is a convenience layer on top. See [Task File Basics](task-files.md) for more on JSON task files.

## Step 4: Validate and Visualize

Before running on a production server, validate your task file:

```bash
rushti tasks validate --tasks my-tasks-opt.txt --skip-tm1-check
```

```
Validation Result: VALID
========================================
Info (3):
  - Source: File (my-tasks-opt.txt)
  - Total tasks: 6
  - Validation passed
```

Generate an interactive DAG visualization:

```bash
rushti tasks visualize --tasks my-tasks-opt.txt --output dag.html
```

Open `dag.html` in your browser to see the dependency graph:

<figure markdown="span">
  ![DAG visualization](../assets/images/screenshots/dag-daily-refresh.png){ loading=lazy }
  <figcaption>Interactive DAG — nodes are tasks, arrows show dependencies</figcaption>
</figure>

!!! tip
    Remove `--skip-tm1-check` to also verify that each TI process exists on the TM1 server.

## Step 5: Save Results (Optional)

Export execution results to a CSV file:

```bash
rushti run --tasks my-tasks-opt.txt --max-workers 4 --result results.csv
```

## What's Next?

<div class="grid cards" markdown>

-   :material-file-document-outline:{ .lg .middle } __Task File Basics__

    ---

    Learn about TM1 cube-based task files, JSON format, and when to use each approach.

    [:octicons-arrow-right-24: Learn more](task-files.md)

-   :material-graph-outline:{ .lg .middle } __DAG Execution__

    ---

    Understand how RushTI schedules tasks based on dependencies — and why DAG is faster than levels.

    [:octicons-arrow-right-24: Learn more](../features/dag-execution.md)

-   :material-auto-fix:{ .lg .middle } __Self-Optimization__

    ---

    RushTI learns from each run to reorder tasks for better parallelization.

    [:octicons-arrow-right-24: Learn more](../features/optimization.md)

-   :material-cube-outline:{ .lg .middle } __TM1 Integration__

    ---

    Read task definitions from and write results to TM1 cubes.

    [:octicons-arrow-right-24: Learn more](../features/tm1-integration.md)

</div>
