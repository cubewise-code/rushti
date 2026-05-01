# RushTI Architecture Refactoring Plan

**Status:** Approved 2026-04-30 — work begins on `refactor/architecture-deepening` branch.
**Target:** Reshape `src/rushti/` for maintainability, readability, and supportability before the next batch of features lands.
**Approach:** Sequenced phases. Phases 0–3 are committed; Phase 4 is gated behind a re-evaluation after Phase 3.

## Locked decisions

| Decision | Choice |
|---|---|
| Branch model | Parent branch (`refactor/architecture-deepening`) off `master`, sub-branches per phase merged into parent locally. Single PR to `master` at the end. |
| Phase 0 test scope | Practical: exit code + key file outputs + critical stdout tokens + error-path coverage. Golden files for `write_optimized_taskfile` snapshot. |
| Phase 0 layering | Two layers: `tests/unit/test_cli_dispatch.py` (no TM1) + `tests/integration/test_commands_smoke.py` (TM1 required, reuses existing `tm1_services` fixtures). |
| Review cadence | Single checkpoint after Phase 0 merges into parent. Auto-pilot through Phases 1–3. Final review before parent → `master`. |
| Backwards compat | Preserve only names exported from `src/rushti/__init__.py`. Other moves are clean cuts (no re-export shims). |
| Phase 4 commitment | **Gated.** Re-evaluate after Phase 3 with a concrete recommendation. If skipped, document with an ADR. |
| Off-limits modules | `exclusive.py`, `checkpoint.py`, `tm1_build.py`, `tm1_integration.py`, `tm1_objects.py`, `messages.py`, `utils.py`, `logging.py`, the HTML emitters, the `Task`/`OptimizedTask` domain model. |
| Incidental bugs found mid-refactor | Logged for later review, never fixed inline in a refactor PR. |
| Commit attribution | Standard project commit style (no conventional-commits prefix), with `Co-Authored-By: Claude Opus 4.7 (1M context)` trailer. |

## Goals

1. Concentrate complexity into deep modules — small interfaces, real leverage behind them.
2. Make every subcommand and every backend independently testable.
3. Remove the circular-import workaround between `cli.py` and `commands.py`.
4. Eliminate the duplicate `write_optimized_taskfile` and split-validation paths.
5. Establish a real seam between the `StatsRepository` interface and its SQLite / DynamoDB adapters.

## Non-goals

- Rewriting the Task / DAG domain model.
- Consolidating the three HTML emitters (`dashboard.py`, `optimization_report.py`, `visualization_template.py`).
- Performance work — none of these phases targets runtime speed.
- Changing public CLI surface, settings semantics, or task-file formats.

## Decision gates

Each phase is reviewed and merged independently. Phase 4 has an explicit re-evaluation gate — the friction it targets may already be acceptable after Phases 1–3.

---

## Phase 0 — Safety net (characterization tests)

**Why it goes first:** there are zero unit tests for `commands.py` or `cli.py` today. Every later phase touches code that has never been pinned by a test. Without this phase, refactors are flying blind.

### Scope

Black-box tests that invoke RushTI through its public surface (`argv` → exit code → side effects on disk / SQLite / log output). The goal is **regression detection**, not coverage of internal logic.

### Deliverables

1. `tests/unit/test_cli_dispatch.py` — argv parsing + dispatch routing (no TM1):
   - Legacy positional args (`rushti tasks.txt 4`)
   - Named args (`--tasks-file`, `--max-workers`)
   - `--help`, `--version`, banner output, exit codes
   - Subcommand routing: `tasks expand|visualize|validate|export|push`, `stats *`, `db *`, `build`, `resume`
   - Config-path resolution: CLI flag → `RUSHTI_DIR` → legacy CWD with deprecation warning
2. `tests/integration/test_commands_smoke.py` — full happy/error path per subcommand using real TM1:
   - `run` (legacy TXT + JSON taskfile)
   - `resume` (from saved checkpoint)
   - `tasks export`, `tasks push`, `tasks expand`, `tasks visualize`, `tasks validate`
   - `stats export`, `stats analyze`, `stats optimize`, `stats visualize`, `stats list`
   - `db list`, `db clear`, `db show`, `db vacuum`
   - `build`
   - Each test uses a unique workflow name (`rushti_phase0_smoke_<uuid>`) and tears down stats DB rows on exit. Tests use `}bedrock.server.wait` as the test process to avoid heavy workloads. Marked `requires_tm1`; auto-skip when no `config.ini` is present.
3. `tests/integration/test_optimized_taskfile_snapshot.py` — golden-file snapshot of the **current** output of `write_optimized_taskfile` from both `taskfile_ops.py` and `contention_analyzer.py`. Goldens live in `tests/resources/golden/`. Drift between the two call sites becomes the Phase 2b regression check.
4. Shared fixtures added to `tests/conftest.py`:
   - `unique_workflow_name` (UUID-suffixed, scoped per test)
   - `populated_stats_db` (small SQLite with 2 runs of sample stats — enables `stats *` and `db *` tests without running real workflows)
   - `golden_file` (factory: read/compare/regenerate via `RUSHTI_REGENERATE_GOLDENS=1` env var)
   - Reuses existing `temp_dir`, `temp_file`, `sample_task_content`, `sample_json_taskfile`, `tm1_services` fixtures.

### Exit criteria

- All new tests pass on `master` baseline.
- Every public subcommand has at least one unit dispatch test (exit code, args namespace) and at least one integration smoke test (full happy path).
- Each subcommand has at least one error-path test (bad config, missing file, malformed input).
- Goldens for `write_optimized_taskfile` exist for both call sites.
- Unit-tests CI job runtime increase < 5 seconds.
- Integration-tests CI job runtime increase < 60 seconds.

### Risks

- **Low.** Pure addition; no production code changes.
- Watch-outs: tests must use `tmp_path` for every file write; integration smoke tests must teardown stats DB rows; unique workflow names per test to avoid concurrent-developer collisions.

### Estimate

~1 sub-branch, ~1–2 days of focused work. Pause for user checkpoint after merging into parent.

### Status (after first iteration)

Sub-branch ``refactor/phase-0-safety-net`` landed three commits:

1. **Golden-file safety net** for both ``write_optimized_taskfile``
   functions. Goldens at ``tests/resources/golden/`` pin current behavior
   byte-for-byte. **Critical for Phase 2b**: revealed that the two
   ``write_optimized_taskfile`` functions are *not* duplicates — they
   share a name but solve different problems (EWMA reorder vs.
   contention grouping). Phase 2b will rename rather than merge.
2. **CLI dispatch tests** (``tests/unit/test_cli_dispatch.py``, 25 tests)
   covering ``uses_named_arguments``, ``translate_cmd_arguments``,
   ``parse_arguments``, ``resolve_config_path`` precedence, ``main()``
   --help / --version / bad-input exits, and subcommand routing via
   handler patching. **Critical for Phase 1**.
3. **Pattern smoke tests** (``tests/unit/test_commands_smoke.py``,
   3 tests) demonstrating the full ``argv -> main() -> handler ->
   output`` chain works for ``tasks expand``, ``tasks visualize``, and
   ``db list workflows`` (via ``--settings`` pointing at a populated
   SQLite DB).

**Total: 30 new unit tests. Suite runtime: 2.53s. No regressions**
(637 passed, 5 skipped).

**Known gaps deferred to follow-up**:

- TM1-required smoke tests (``run``, ``resume``, ``build``,
  ``tasks export``, ``tasks push``) — pattern is established; can be
  added in ``tests/integration/test_commands_smoke.py`` as Phase 2
  surfaces specific risks.
- Remaining non-TM1 smoke tests (``tasks validate``, ``stats *``,
  ``db show/clear/vacuum``) — same shape as the three already there;
  add as needed.

**New conftest fixtures**:

- ``unique_workflow_name`` — UUID-suffixed name for integration tests
- ``populated_stats_db`` — tmp SQLite DB with one workflow run + 2 tasks
- ``golden_file`` — read/compare/regenerate helper
  (``RUSHTI_REGENERATE_GOLDENS=1`` overwrites)

**Plan adjustments triggered by Phase 0 findings**:

- Phase 2b's "dedup ``write_optimized_taskfile``" goal is replaced by
  "rename to disambiguate" — same depth gain, lower risk.

---

## Phase 1 — Extract non-CLI helpers from `cli.py`

**Why it goes second:** removes the circular-import workaround that `commands.py` uses, which is a prerequisite for cleanly splitting `commands.py` in Phase 2.

### Scope

`cli.py` is 1376 lines and owns several responsibilities that are not CLI parsing. Extract them into named modules.

### Deliverables

| New module | Moves from `cli.py` |
|---|---|
| `src/rushti/app_paths.py` | `resolve_config_path`, `_legacy_path_warnings`, `log_legacy_path_warnings`, `RUSHTI_DIR` lookup logic |
| `src/rushti/logging_setup.py` | `_resolve_logging_config`, `add_log_level_arg`, `apply_log_level` |
| `src/rushti/results_writer.py` | `create_results_file`, the CSV portion of `exit_rushti` |

Stays in `cli.py`: `print_banner`, `create_argument_parser`, `parse_named_arguments`, `uses_named_arguments`, `parse_arguments`, `translate_cmd_arguments`, `main`.

### Mechanical steps

1. For each new module: copy functions out of `cli.py`, add an `__all__`, port the imports.
2. Update import sites across `commands.py`, `taskfile_ops.py`, etc.
3. Delete every lazy `from rushti.cli import ...` inside function bodies in `commands.py`.
4. Run Phase 0 tests — they must still pass unchanged.

### Exit criteria

- `grep -rn "from rushti.cli import" src/rushti/` returns only `commands.py` (or zero callers if `main()` is the only entry).
- No lazy imports left in `commands.py`.
- Phase 0 tests green.
- `cli.py` shrinks by ≥ 400 lines.

### Risks

- **Low.** File moves with import updates.
- Watch for: `RUSHTI_DIR` env-var precedence — there's a tested fallback chain we mustn't reorder.

### Estimate

~1 PR, ~1 day.

---

## Phase 1 — Status (after sub-branch merge into parent)

Sub-branch ``refactor/phase-1-cli-cleanup`` merged into parent. Three
focused modules extracted from cli.py:

- ``app_paths.py`` (89 lines): config-path resolution + legacy-warning tracking
- ``logging_setup.py`` (130 lines): logging config preprocessing + log-level helpers
- ``results_writer.py`` (60 lines): CSV results-summary writer

Lazy ``from rushti.cli import …`` calls inside ``commands.py`` reduced
from 5 to 2 (the remaining two reference names that legitimately still
live in cli.py — ``CONFIG`` and ``add_taskfile_source_args``).

cli.py: 1376 → 1177 lines (-199). The plan target of 400-line reduction
was over-optimistic; most of cli.py is ``main()`` and
``create_argument_parser()``, which Phase 1 left in place per the plan.

All 637 unit tests still pass. Suite runtime: 2.48s.

---

## Phase 2 — Split `commands.py` and dedup taskfile operations

**Why it goes here:** the biggest readability win. With Phase 1's helpers extracted, each subcommand can move into its own module without dragging in cli-state.

### Sub-phase 2a — Split `commands.py`

Target structure:

```
src/rushti/commands/
    __init__.py            # public exports: run_*_command functions
    build.py               # was: run_build_command
    resume.py              # was: run_resume_command
    tasks/
        __init__.py        # was: run_tasks_command (dispatcher only)
        export.py          # was: _tasks_export
        push.py            # was: _tasks_push
        expand.py          # was: _tasks_expand
        visualize.py       # was: _tasks_visualize
        validate.py        # was: _tasks_validate
    stats/
        __init__.py        # was: run_stats_command (dispatcher only)
        export.py          # was: _stats_export
        analyze.py         # was: _stats_analyze
        optimize.py        # was: _stats_optimize
        visualize.py       # was: _stats_visualize
        list.py            # was: _stats_list
    db.py                  # was: run_db_command
```

`commands/__init__.py` keeps `commands.run_build_command` etc. importable from the original path — backwards compat for any caller that imports them directly.

### Sub-phase 2b — Disambiguate `write_optimized_taskfile`

**Plan revised after Phase 0 finding:** the two functions sharing the
name `write_optimized_taskfile` are not duplicates — they have different
signatures and solve different problems:

- `taskfile_ops.write_optimized_taskfile(path, optimized_order, output, AnalysisReport)` — EWMA-based reordering by task-ID list.
- `contention_analyzer.write_optimized_taskfile(path, ContentionAnalysisResult, output)` — contention-driver grouping with predecessor injection and `max_workers` embedding.

The original "merge into one method" plan would have produced an
overloaded API. Better:

- Rename to `taskfile_ops.write_ewma_optimized_taskfile`.
- Rename to `contention_analyzer.write_contention_optimized_taskfile`.
- Optionally: hoist both to methods on `Taskfile` (`with_ewma_order`, `with_contention_optimization`) so the model owns its operations.
- Phase 0 golden-file snapshots guard against any byte-level drift during the rename.

### Sub-phase 2c — Consolidate validation

Today: `validate_taskfile` in `taskfile.py` (structural) and `validate_taskfile_full` in `taskfile_ops.py` (structural + TM1 connectivity).

- `Taskfile.validate()` → structural only (the existing `validate_taskfile` logic).
- `taskfile_ops.validate_taskfile_full(taskfile, tm1_services)` → keeps TM1 reachability check; calls `Taskfile.validate()` first.
- Clear separation: model knows its shape; `taskfile_ops` knows how it interacts with TM1.

### Exit criteria

- `wc -l src/rushti/commands/*.py src/rushti/commands/**/*.py` averages < 300 lines per file.
- Phase 0 snapshot tests pass byte-for-byte against optimized-taskfile output (or any drift is intentional and documented).
- Single definition of `write_optimized_taskfile` and `validate_taskfile` in the codebase.
- All Phase 0 + Phase 1 tests green.

### Risks

- **Medium.** Snapshot tests must run first — if the two `write_optimized_taskfile`s have drifted, this phase will reveal it.
- Watch for: the `--analyze` and `--optimize` paths take different sub-options today; the merged method must accept both.

### Estimate

2 PRs (2a alone, then 2b+2c together), ~3–4 days total.

### Status — complete

Sub-branches landed sequentially into the parent
``refactor/architecture-deepening``:

1. **Phase 2a-1/2** (sub-branch ``refactor/phase-2-commands-split``)
   - ``commands.py`` → ``commands/__init__.py`` (package layout).
   - ``build``, ``resume``, ``db`` subcommands extracted into
     ``commands/build.py``, ``commands/resume.py``, ``commands/db.py``.
2. **Phase 2a-3/4** (sub-branch
   ``refactor/phase-2a-tasks-stats-split``)
   - ``run_tasks_command`` (469 lines) split into
     ``commands/tasks/{__init__,export,push,expand,visualize,validate}.py``.
   - ``run_stats_command`` (~1010 lines) split into
     ``commands/stats/{__init__,export,analyze,optimize,visualize,list}.py``.
3. **Phase 2b+2c** (sub-branch
   ``refactor/phase-2b-rename-optimized``)
   - ``taskfile_ops.write_optimized_taskfile`` → ``write_ewma_optimized_taskfile``.
   - ``contention_analyzer.write_optimized_taskfile`` →
     ``write_contention_optimized_taskfile``.
   - Updated all 7 importer call sites + tests; Phase 0 golden
     snapshots remained byte-identical (rename only, no logic change).
   - Added ``Taskfile.validate()`` method that delegates to the
     existing ``validate_taskfile`` free function.
   - ``validate_taskfile_full`` now calls ``Taskfile.validate()`` for
     the Taskfile and TaskfileSource code paths.

**End state**:

- ``commands/__init__.py``: 2113 → 32 lines (a thin facade
  re-exporting all five subcommand handlers).
- Two ``write_optimized_taskfile`` definitions disambiguated by name.
- Structural validation owned by the model; TM1-reachability
  validation owned by ``taskfile_ops``.
- All 637 unit tests pass after every commit.

---

## Phase 3 — Stats Protocol + adapter split

**Why it goes after the structural cleanup:** with `commands.py` split and helpers extracted, the type-hint changes in this phase touch fewer places at once.

### Scope

Make the SQLite vs DynamoDB seam real instead of duck-typed.

### Target structure

```
src/rushti/stats/
    __init__.py            # re-exports for backwards compat
    repository.py          # StatsRepository Protocol; create_stats_database factory
    sqlite.py              # StatsDatabase (was stats.py:70-834)
    dynamodb.py            # DynamoDBStatsDatabase (was stats.py:836-1369)
    signature.py           # calculate_task_signature
    paths.py               # get_db_path, get_stats_backend, DEFAULT_DB_PATH, DEFAULT_STATS_BACKEND
```

### Protocol surface

`StatsRepository` exposes the methods both backends already implement:

```
start_run, record_task, batch_record_tasks, complete_run, cleanup_old_data,
get_task_history, get_workflow_signatures, get_task_sample_count,
get_task_durations, get_run_results, get_run_info, get_runs_for_workflow,
get_all_runs, get_run_task_stats, get_concurrent_task_counts, close,
__enter__, __exit__
```

Use `typing.Protocol` (PEP 544) — structural typing, no inheritance change to existing classes.

### Importer migration

8 modules import `StatsDatabase` directly today. After Phase 3:
- Type hints become `StatsRepository`.
- Construction goes through `create_stats_database(...)`.
- `db_admin.py` keeps its SQLite-only import (it's intentionally backend-specific) but with a clear comment.

### New test capability

`tests/unit/test_stats_repository.py` defines a `FakeStatsRepository` and runs a parity smoke test against both real backends. Optimizer / contention-analyzer tests can now use the fake instead of touching SQLite.

### Exit criteria

- `StatsRepository` Protocol exists; both adapters satisfy it (verified by mypy or runtime structural check).
- Existing 1000-line `test_stats.py` passes unchanged.
- At least one optimizer test uses the fake adapter (proof the seam is useful).
- `from rushti.stats import StatsDatabase` still works (backwards compat re-export).

### Risks

- **Medium.** Touches 8 importers. Most are mechanical type-hint changes.
- Watch for: `getattr(stats_db, "backend", ...)` runtime attribute reads — confirm both adapters expose them.
- Watch for: `db_admin.py` deliberately uses SQLite-only operations (`vacuum`, raw SQL queries). Don't promote those to the Protocol.

### Estimate

~1 PR, ~2 days.

### Status — complete

Sub-branch ``refactor/phase-3-stats-protocol`` merged into parent.
``stats.py`` (1453 lines) split into a focused ``stats/`` package:

- ``stats/__init__.py``    — re-exports for backwards compat
- ``stats/paths.py``       — ``DEFAULT_*`` constants, ``get_db_path``,
                             ``get_stats_backend``
- ``stats/signature.py``   — ``calculate_task_signature``
- ``stats/sqlite.py``      — ``StatsDatabase`` (SQLite adapter)
- ``stats/dynamodb.py``    — ``DynamoDBStatsDatabase`` (DynamoDB adapter)
- ``stats/repository.py``  — ``StatsRepository`` Protocol +
                             ``create_stats_database`` factory

``StatsRepository`` is a ``runtime_checkable`` ``typing.Protocol``;
both real adapters satisfy it structurally with no inheritance change.

**Importer migration** (5 of 8 files; ``db_admin.py`` intentionally
not migrated — it uses raw sqlite3 connections for SQLite-only
operations; ``execution.py`` only carried a docstring reference):

- ``tm1_integration.py`` — type hints + docstrings
- ``taskfile_ops.py``    — type hints + docstrings
- ``optimizer.py``       — type hints + docstrings
- ``contention_analyzer.py`` — replaced the ``AnyStatsDatabase``
  Union shim with ``StatsRepository``; alias retained for callers.
- ``execution.py``       — docstring only

**New test capability**: ``tests/unit/test_stats_repository.py`` (5
tests) defines a ``FakeStatsRepository`` that satisfies the Protocol
and drives ``TaskOptimizer`` end-to-end without touching SQLite or
DynamoDB. Both real adapters are also verified via ``isinstance``.

Suite size: 637 → 642 tests, all passing.

---

## Phase 4 — Re-cut `parsing.py` and `taskfile.py` (decision gate)

**Re-evaluate before starting.** After Phases 1–3, the friction this phase targets may have dropped enough not to justify the disruption. The deletion test is the gate: if `parsing.py` still feels load-bearing across multiple callers after Phase 2's `Taskfile` model deepening, proceed. Otherwise skip.

### Scope (if proceeding)

Today the split is "TXT-handling vs JSON-handling smeared across both files." Pivot to "model + format adapters vs DAG construction."

### Target structure

```
src/rushti/taskfile/
    __init__.py            # re-exports Taskfile, TaskDefinition, etc.
    model.py               # Taskfile dataclass, methods, validation
    txt_format.py          # TXT parsing; Wait class becomes private here
    json_format.py         # JSON parsing
    convert.py             # convert_txt_to_json
src/rushti/dag_build.py    # build_dag, convert_json_to_dag (was parsing.py)
```

`Wait` moves from `task.py` into `txt_format.py` as a module-private sentinel. `task.py` shrinks to the pure domain types: `Task`, `OptimizedTask`, `ExecutionMode`.

### Risks

- **Highest of any phase.** Parsing is the public-facing surface for users with custom taskfiles. Any behavior change ships.
- Backwards compat for `from rushti.taskfile import ...` — must preserve every public name via `__init__.py` re-exports.
- Backwards compat for `from rushti.parsing import build_dag, ...` — same.

### Exit criteria

- All Phase 0–3 tests green.
- A representative legacy TXT taskfile and JSON taskfile from `docs/samples/` both `run` to identical results before/after.
- `Wait` is not importable from `rushti.task` (it's now an internal detail of TXT parsing).

### Estimate

~1 PR, ~3–4 days.

### Decision — SKIP (2026-05-01)

After Phases 1–3 merged into parent the gate was re-evaluated and
Phase 4 is **skipped**. Rationale and trade-offs are documented in
**[ADR-0001: Skip Phase 4 of the architecture refactor](adr/0001-skip-phase-4-parsing-recut.md)**.
Summary:

- The deletion test passes structurally (``parsing.py`` has 2 external
  callers — ``cli.py`` and ``execution.py``) but the remaining
  friction is naming/aesthetic, not architectural.
- Phases 1–3 captured the major structural wins (deep modules,
  removed circular imports, real adapter seam, model-owned
  validation). The Phase 4 risk profile (per the original plan,
  "Highest of any phase. Parsing is the public-facing surface for
  users with custom taskfiles.") is disproportionate to the
  remaining payoff.
- Revisit only if a future feature genuinely needs the
  format-adapter split (e.g., adding a YAML format).

---

## Cross-cutting concerns

### Backwards compatibility

- Every public name moved between modules gets a re-export at the original import path. We don't break `from rushti.cli import resolve_config_path` even if the body now lives in `app_paths.py`.
- Public CLI surface (subcommands, flags, exit codes) is unchanged across all phases.
- Config-file formats unchanged.

### Documentation

After each phase, update:
- `docs/architecture/design.md` — module dependency graph, Core Components table.
- `docs/architecture/contributing.md` — Project Structure tree.

`README.md` does not need updates — it doesn't reference internal module structure.

### Performance check

After Phase 3, run a quick before/after timing on a 100-task workflow:
- `rushti run sample.json` cold start
- `rushti tasks expand sample.json`

We don't expect changes; the check is a guardrail against accidental regressions (e.g., an import-order change adding 200ms of startup).

### CI / release

- Phases land as sub-branches into `refactor/architecture-deepening` (parent branch).
- Parent branch merges to `master` as a single PR after the final-review gate.
- The unit-tests CI job (every push) covers `tests/unit/test_cli_dispatch.py`.
- The integration-tests CI job (PRs to master only, requires TM1 secrets) covers `tests/integration/test_commands_smoke.py` and the golden-file snapshot.
- No version bump until the final PR. Optional pre-release tag (`2.2.0-rc`) at parent → `master` time if you want a checkpoint before Phase 4.

### What if a feature lands mid-refactor?

Each phase is small enough (~1–2 days) that the right answer is "merge the phase first, then the feature." If a feature is urgent, pause the refactor at a phase boundary — never mid-phase.

---

## Suggested calendar

Assuming one engineer, no other interrupts:

| Phase | Effort | Cumulative |
|---|---|---|
| 0. Safety net | 1–2 days | Day 2 |
| 1. cli.py cleanup | 1 day | Day 3 |
| 2. commands split + dedup | 3–4 days | Day 7 |
| 3. Stats Protocol | 2 days | Day 9 |
| **Decision gate** | — | — |
| 4. Parsing re-cut (if proceeding) | 3–4 days | Day 13 |

Realistic with reviews and interrupts: 2–3 weeks for Phases 0–3; another week if Phase 4 proceeds.

## Success metrics

After Phases 0–3:
- `commands.py` no longer exists as a single file (replaced by `commands/` package).
- No file in `src/rushti/` exceeds 1000 lines (today: 5 do).
- Zero lazy imports inside function bodies in `src/rushti/`.
- Two stats adapters both satisfy a documented `StatsRepository` Protocol.
- Every subcommand has at least one black-box test.
- `wc -l src/rushti/cli.py` < 800 (today: 1376).

After Phase 4 (if proceeding):
- `Wait` is not importable from `rushti.task`.
- `parsing.py` is gone; format-specific code lives in `taskfile/{txt,json}_format.py`.
