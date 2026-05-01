# ADR-0001: Skip Phase 4 of the architecture refactor

**Status:** Accepted
**Date:** 2026-05-01
**Decider:** Project lead (after Phase 3 gate review)
**Context for:** the architecture refactor plan tracked in
[refactoring-plan.md](../refactoring-plan.md)

## Context

The architecture refactor plan defined four sequenced phases:

| Phase | Scope | Status |
|---|---|---|
| 0 | Safety net (characterization tests) | Merged |
| 1 | Extract non-CLI helpers from `cli.py` | Merged |
| 2 | Split `commands.py` + dedup taskfile operations | Merged |
| 3 | Stats `Protocol` + SQLite/DynamoDB adapter split | Merged |
| **4** | **Re-cut `parsing.py` and `taskfile.py`** | **Gated** |

Phase 4 was deliberately gated. The original plan reads:

> Re-evaluate before starting. After Phases 1–3, the friction this
> phase targets may have dropped enough not to justify the
> disruption. The deletion test is the gate: if `parsing.py` still
> feels load-bearing across multiple callers after Phase 2's
> `Taskfile` model deepening, proceed. Otherwise skip.

> Phase 4 commitment: **Gated.** Re-evaluate after Phase 3 with a
> concrete recommendation. If skipped, document with an ADR.

This ADR records the re-evaluation outcome.

## Phase 4 — what it would have done

Pivot the parsing layout from "TXT-handling vs JSON-handling smeared
across both files" to "model + format adapters vs DAG construction":

```
src/rushti/taskfile/
    __init__.py            # re-exports Taskfile, TaskDefinition, etc.
    model.py               # Taskfile dataclass, methods, validation
    txt_format.py          # TXT parsing; Wait class becomes private here
    json_format.py         # JSON parsing
    convert.py             # convert_txt_to_json
src/rushti/dag_build.py    # build_dag, convert_json_to_dag (was parsing.py)
```

Plus: move `Wait` from `task.py` into `txt_format.py` as a
module-private sentinel. `task.py` shrinks to the pure domain types
(`Task`, `OptimizedTask`, `ExecutionMode`).

Estimate per the plan: 1 PR, ~3–4 days. The plan also flagged this as
*the highest-risk phase*: parsing is the public-facing surface for
users with custom taskfiles, and any behavior change ships.

## Re-evaluation — the deletion test

The gate criterion was: *does `parsing.py` still feel load-bearing
across multiple callers after Phase 2?*

Findings (as of `refactor/architecture-deepening` parent branch):

- `parsing.py` (413 lines) is imported by 2 production modules:
  - `cli.py` — `convert_json_to_dag`, `build_dag` (DAG construction
    at run-time)
  - `execution.py` — `get_instances_from_tasks_file` (used during
    exclusive-lock pre-flight)
- Internally `parsing.py` mixes TXT-line extraction
  (`extract_task_or_wait_from_line`, `expand_task`, ...) with JSON-to-DAG
  conversion (`convert_json_to_dag`) and the public `build_dag`
  dispatcher.
- `taskfile.py` (723 lines) already owns format conversion
  (`parse_json_taskfile`, `convert_txt_to_json`, `detect_file_type`).
  The "smear" is at the function-level *inside* `parsing.py`, not at
  the module boundary.
- Phase 2c added `Taskfile.validate()`, deepening the model further.
  Structural validation is now a method on the dataclass.

The deletion test passes *structurally* (yes, multiple callers, real
work) — but the remaining friction is **naming and aesthetics**
(co-located TXT and JSON DAG functions inside `parsing.py`), not
architectural. The call graphs are correct; the module names are
historically accurate; the extra `Wait` import in `dag.py` is benign.

## Decision

**Skip Phase 4.** Phases 1–3 already delivered the structural payoff:

- `cli.py`'s helper pile is gone (Phase 1).
- `commands.py` is no longer a single 2113-line file — every
  subcommand has its own focused module under `commands/` (Phase 2a).
- The two name-clashed `write_optimized_taskfile` functions are
  disambiguated; the model owns structural validation (Phase 2b/2c).
- The SQLite vs DynamoDB seam is real: both adapters satisfy a
  documented `StatsRepository` Protocol and a fake adapter is enough
  to drive `TaskOptimizer` end-to-end (Phase 3).

Phase 4 would deliver:

- A nicer module layout for parsing (real benefit: easier to read,
  easier to add a third format like YAML).
- `Wait` no longer leaking out of `rushti.task` (real benefit: the
  domain model gets cleaner; the leak is only used by `dag.py`'s TXT
  path).

…against the plan's own risk note: *"Highest of any phase. Parsing
is the public-facing surface for users with custom taskfiles. Any
behavior change ships."*

The risk-to-payoff ratio after Phases 1–3 has flipped: the gain is
incremental polish, the cost is ~1 week of churn in a user-facing
module surface, with backwards-compat shims to maintain on every
public name moved.

## Consequences

### Accepted

- `parsing.py` keeps its current name and shape. The function-level
  smear of TXT and JSON code remains internal to one file.
- `Wait` continues to be importable from `rushti.task`. `dag.py`
  keeps its `from rushti.task import Wait`.
- The `taskfile.py` and `parsing.py` import paths stay stable for
  any external code that depends on them.
- Phase 0 golden snapshots and the structural `Taskfile.validate()`
  method are sufficient guards against accidental regressions in
  these modules.

### Rejected (will not happen as part of this refactor)

- The `taskfile/{model,txt_format,json_format,convert}.py` package
  layout.
- Renaming `parsing.py` → `dag_build.py`.
- Making `Wait` private to TXT parsing.

### Revisit triggers

This decision should be reconsidered if **any** of these conditions
arise:

1. A new taskfile format (e.g., YAML) is genuinely required —
   currently the friction is bearable because TXT + JSON both already
   work. A third format would make the smear actively painful.
2. `parsing.py` exceeds ~600 lines or accumulates significantly more
   "TXT vs JSON" branches.
3. A bug surfaces that's directly traceable to the format-handling
   smear (e.g., a TXT-only fix accidentally breaking JSON callers
   because the two paths share too much).

In any of those cases the existing plan can be revived from
[refactoring-plan.md § Phase 4](../refactoring-plan.md). The Phase 0
goldens for `write_*_optimized_taskfile` would still apply, and the
mechanical work itself is well-scoped.

## Related

- [Architecture refactor plan](../refactoring-plan.md)
- Phase 0 safety-net: `tests/unit/test_optimized_taskfile_snapshot.py`
- Phase 2c: `Taskfile.validate()` in `src/rushti/taskfile.py`
- Phase 3: `StatsRepository` Protocol in
  `src/rushti/stats/repository.py`
