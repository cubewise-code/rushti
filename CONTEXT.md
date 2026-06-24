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
A single execution unit inside a taskfile. Each task has:
- `id` — a positive integer string, used as an element name in
  `rushti_task_id`.
- `instance` — **the TM1 instance where this task executes** (task-level).
- exactly one **kind field** — either `process` (TI process) or `chore`
  (TM1 chore). The field name *is* the discriminator; there is no
  meta-`kind` field. See [[adr/0002-polymorphic-task-kinds]].
- kind-specific and shared optional fields — see **Task kind** below.

### Task kind
RushTI supports two task kinds, identified by which field names the
execution target:

| Kind | Field | Applicable optional fields |
|---|---|---|
| **process** (TI process) | `process` | `parameters`, `succeed_on_minor_errors`, `timeout`, `cancel_at_timeout`, `safe_retry`, plus shared (below) |
| **chore** (TM1 chore) | `chore` | `safe_retry` (only when chore is `SINGLE_COMMIT`), plus shared |

**Shared optional fields** (apply to both kinds): `predecessors`, `stage`,
`require_predecessor_success`.

A task with both `process` and `chore` set is invalid; a task with
neither is invalid. The invariant is enforced at parse-time validation
*and* as a class invariant in `Task.__init__`.

Chores are intentionally narrower than processes:
- **No parameters** — TM1 chores have no invocation parameters.
- **No timeout / cancel** — TM1 chore execution has no native timeout.
- **No minor-error tier** — chore execution is binary at the API
  boundary (HTTP 204 = success, 500 = failure).
- **Retry is whole-chore.** When `safe_retry: true` on a chore task,
  retry re-executes the entire chore. Restricted to SINGLE_COMMIT
  chores so partial state never leaks on failure.

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
- TM1 connection parameters — `config.ini` only (the *values*; the file's
  *location* is resolved separately, see below).

---

## config.ini location resolution

`config.ini` holds TM1 connection parameters only; its *location* is resolved
by `resolve_config_path("config.ini", cli_path=…)` (`app_paths.py`) in this
order (highest wins):

1. **`--config` CLI flag** — explicit path to a `config.ini` file. Present on
   every TM1-connecting command (`run`, `build`, `tasks …`, `resume`); not on
   `stats`/`db`, which touch only local SQLite + settings.ini. A missing path
   fails fast with a clean error, no traceback.
2. **`RUSHTI_DIR` env var** — looks in `{RUSHTI_DIR}/config/config.ini`.
3. **Legacy CWD** — `./config.ini` (deprecated, warns once).
4. **`config/config.ini`** — the recommended default location.

`--config` relocates **only `config.ini`** — settings.ini keeps `--settings`,
logging_config.ini keeps its own resolution, and `RUSHTI_DIR` still governs
those siblings. The flag exists so RushTI can share one read-only `config.ini`
with other tm1py utilities (e.g. OptimusPy) instead of duplicating it. When
`--config` is absent, resolution is unchanged from prior behaviour.

The resolved path is **threaded explicitly** as `config_path` into the TM1
connection layer (`connect_to_tm1_instance`, `setup_tm1_services`); the
module-level `CONFIG` global in `cli.py` is now only the no-flag default.

---

## Files of record

| File | Purpose |
|---|---|
| `config/settings.ini` | Execution defaults (user-editable). |
| `config/settings.ini.template` | Documented example, shipped in repo. |
| `config/config.ini` | TM1 connection parameters per instance. |
| `archive/{workflow}/{run_id}.json` | Snapshot of the taskfile actually executed, for audit + DAG reconstruction. |
| `data/rushti_stats.db` | Local SQLite stats database (when `[stats] enabled = true`). |
