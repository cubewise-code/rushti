# RushTI — Domain Context

This file captures the canonical vocabulary used in RushTI. Terms here are
load-bearing: they appear in code, docs, settings, and user-facing messages.
When a term feels ambiguous in conversation, anchor it to a definition here.

> Scope: single context. No `CONTEXT-MAP.md` — the whole repository shares
> this vocabulary.

---

## Core terms

### Taskfile
The structured input that drives a `rushti run`. Materialised as either:
- a **JSON task file** (`.json`) parsed by `parse_json_taskfile`, OR
- a **TXT task file** (`.txt`) converted on read by `convert_txt_to_json`, OR
- a **TM1 cube taskfile** read by `read_taskfile_from_tm1` (an MDX query
  against the `rushti` cube using the `Input` element of `rushti_run_id`).

The in-memory shape is the `Taskfile` dataclass (`taskfile.py`) — a container
with `version`, `metadata`, `settings`, and `tasks`. Always prefer
**"taskfile"** (one word, lowercase) in docs; **"JSON task file"** is also
acceptable when the format matters.

Do **not** call it `workflow.json` — that name implies the file *is* a
workflow, which conflates the container with one of its metadata fields.

### Workflow
The logical identifier for a run, stored in `metadata.workflow` of a taskfile
and as an element of the `rushti_workflow` dimension. A taskfile contains
tasks *for* one workflow.

Use **"workflow"** as an adjective for scope ("workflow-level setting",
"per-workflow override") rather than as a synonym for "taskfile".

### Task
A single TI process execution inside a taskfile. Each task has:
- `id` — a positive integer string, used as an element name in
  `rushti_task_id`.
- `instance` — **the TM1 instance where this task executes** (task-level).
- `process` — the TI process name on that instance.
- `parameters`, `predecessors`, `stage`, `timeout`, etc.

### TM1 instance
A named TM1 server defined in `config.ini`. Three roles can apply
contextually — context disambiguates, not the name:

| Role | Meaning | Where it shows up |
|---|---|---|
| **Source** | Where a taskfile is read *from* (cube source) | `rushti run --tm1-instance X` |
| **Execution target** | Where a task executes | task-level `instance` field |
| **Results target** | Where execution results are pushed | settings.ini or taskfile `tm1_instance`; also CLI `--tm1-instance` as fallback |

The canonical setting key for the results target is `tm1_instance` (in both
`settings.ini [tm1_integration]` and the taskfile `settings` block). The
deprecated alias is `default_tm1_instance`.

There is no separate "source TM1 instance" override — `--tm1-instance` is
both the source and (in absence of higher-precedence overrides) the results
target for a `run` invocation.

### Source vs target — naming convention
RushTI does not prefix instance names with `source_` or `target_`. The
*command* or the *config section* makes the role obvious:
- `rushti run --tm1-instance X` — X is the source for cube-read, and the
  fallback results target if nothing else is set.
- `rushti tasks push --tm1-instance X` — X is the target of the push
  (canonical form). The legacy alias `--target-tm1-instance` is deprecated.
- `[tm1_integration].tm1_instance` — the section header makes "target" clear.

This convention is intentional: prefixes don't scale across surfaces and
fight against contextual disambiguation. See
[[adr/0001-tm1-instance-resolution]] for the full rationale.

---

## Settings precedence

The effective value for a settings-driven knob is resolved in this order
(highest wins):

1. **CLI arguments** — e.g. `--max-workers`, `--tm1-instance`.
2. **Taskfile settings block** — e.g. `{"settings": {"max_workers": 8}}`
   in JSON. Applied via `_apply_json_settings` inside
   `get_effective_settings` *after* the taskfile is parsed.
3. **`settings.ini`** — the canonical defaults file at
   `config/settings.ini`.
4. **Built-in defaults** — the dataclass field defaults in
   `settings.py`.

Three knobs are *not* settings-driven and don't follow this chain:
- Per-task `instance` and `process` — taskfile-only, no fallback.
- TM1 connection parameters — `config.ini` only.

---

## Files of record

| File | Purpose |
|---|---|
| `config/settings.ini` | Execution defaults (user-editable). |
| `config/settings.ini.template` | Documented example, shipped in repo. |
| `config/config.ini` | TM1 connection parameters per instance. |
| `archive/{workflow}/{run_id}.json` | Snapshot of the taskfile actually executed, for audit + DAG reconstruction. |
| `data/rushti_stats.db` | Local SQLite stats database (when `[stats] enabled = true`). |
