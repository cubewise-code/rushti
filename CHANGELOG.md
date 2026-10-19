# Changelog

All notable changes to RushTI are documented in this file.

## [Unreleased]

- **Added: run phase time limits** (closes #124). A run goes through four
  run phases (`task_retrieval`, `connection`, `task_execution`,
  `push_results`), and each can now have a time limit in seconds: a
  `[timeouts]` section in `settings.ini`, `settings.timeouts` in the taskfile,
  or `--timeout-task-retrieval`, `--timeout-connection`,
  `--timeout-task-execution` and `--timeout-push-results` on `run` and
  `resume`. A phase that runs past its limit is a hang: RushTI logs which limit
  was hit, writes the result file with `Status` = `TimedOut`, keeps the
  checkpoint for `rushti resume`, logs out of TM1 in the background and exits
  with code 1, so a TI process that called RushTI with `ExecuteCommand` stops
  waiting. TI processes that were already running are not stopped: they run to
  completion on the server and commit, while the checkpoint shows them as in
  progress. Off by default. The exclusive-mode wait doesn't count against `connection`. See
  [Run Phase Time Limits](docs/features/timeouts.md). The docs now also state
  that per-task `cancel_at_timeout` doesn't cancel the process on TM1 v12,
  where TM1py blocks its thread calls.
- **Added: `Status` column in the `--result` file.** Every run outcome now
  writes `Success`, `Failed` (at least one task failed), `Aborted` (stopped on
  an error, including the exclusive-mode wait running out) or `TimedOut`. The
  column is added at the end of the row; the existing columns are unchanged.
  **If a TI process reads the result file with a fixed set of variables, check
  that it still accepts the extra column.**
- **Fixed: a failed `}rushti.load.results` marked the whole run as aborted.**
  With `auto_load_results = true`, the load's outcome overwrote the run's own,
  so a load failure (logged as a non-blocking warning) wrote
  `Overall Success` = `False`. It no longer affects the run's outcome.
- **Fixed: `--result` file not written when a run aborts** (part of #124).
  The result file was only written when RushTI reached the end of a run, so a
  calling TI process couldn't tell an aborted run from one that never ran. It
  is now written on every exit of a run: fatal errors during execution, a
  missing `--config`, an invalid taskfile, a failed TM1 taskfile read and an
  exclusive-mode timeout. Aborted runs report `Overall Success` = `False`. When
  RushTI aborts before reading the taskfile, the path comes from `--result` or
  `settings.ini`, because a `result_file` set inside the taskfile isn't known
  yet.

## [2.3.2] - 2026-07-09

- **Fixed: results written to the wrong measures after an in-place upgrade**
  (closes #169). `}rushti.load.results` maps CSV columns to TI variables
  positionally. A cube/process built by a pre-2.3.0 rushti (before the `chore`
  column) that was then upgraded without re-running `rushti build` would
  silently shift every value from `chore`/`parameters` onward into the next
  measure. The loader now reads the CSV header row (`asciiHeaderRecords=0`) and
  validates each column against the variable it feeds in the metadata tab: on
  mismatch it logs the offending columns and `ProcessQuit`s instead of writing
  scrambled data. **After upgrading rushti you must re-run
  `rushti build --tm1-instance X`** to refresh the process (and additively add
  any new measure element) — the guard only turns future drift into a clear
  error, it does not repair an already-stale process. Existing rows written
  under the wrong measures should be cleared and the workflow re-run.

## [2.3.1] - 2026-06-26

- **Added: `--config PATH` CLI flag** (closes #164). Overrides the location of
  `config.ini` (TM1 connection parameters) for a single invocation, on every
  TM1-connecting command (`run`, `build`, `tasks …`, `resume`). Precedence:
  `--config` > `RUSHTI_DIR` > legacy CWD > `config/`. The flag relocates only
  `config.ini` — `settings.ini` (`--settings`) and `logging_config.ini` keep
  their own resolution. A missing path fails fast (exit 1, no traceback). The
  resolved path is threaded explicitly into the TM1 connection layer rather
  than mutating a global. Enables sharing one read-only `config.ini` with other
  tm1py utilities. No behavioural change when the flag is absent. See
  `docs/adr/0003-config-ini-location-resolution.md`.

## [2.3.0] - 2026-06-17

- **Added: TM1 chore execution as a first-class task kind** (closes #156).
  Mixed process + chore taskfiles are now supported across JSON, TXT, and
  cube sources. A task carries exactly one of `process` or `chore` — the
  field name is the discriminator. Chores are intentionally narrower than
  processes: no parameters, no minor-error tier, no native timeout; only
  `safe_retry` is honoured (and restricted to `SINGLE_COMMIT` chores so
  partial state cannot leak on failure). See `docs/adr/0002-polymorphic-task-kinds.md`
  for the full rationale.
- Added: `chore TEXT` column on the `task_results` SQLite table.
  Auto-migrated on first connection — no manual step required.
- **Behavioural change:** `}rushti.load.results` TI schema changed. A new
  `chore` measure element is added to the cube and a matching `vchore`
  variable to the TI variable list. CSVs written by the previous version
  cannot be loaded by the new TI. Mitigation: flush any pending CSVs
  before upgrade (default `pDeleteSourceFile=1` makes CSVs transient in
  practice), then re-run `rushti build --tm1-instance X` so the additive
  merge adds the new measure element to the existing cube.
- **Behavioural change:** TXT taskfiles now run through `validate_taskfile`
  on read. Pre-existing malformed TXT files (missing fields, wrong types,
  chore tasks carrying process-only fields) that previously parsed
  silently will now fail with explicit error messages. This closes a
  long-standing gap; the fix surfaces bugs that were always present.
- Dashboard: the per-task "Process" column is replaced by a unified
  "Task target" column with a `[P]` / `[C]` kind indicator so process
  and chore rows render side-by-side.
- **Docs fix:** corrected the long-standing claim that `--mode` is
  deprecated/ignored (#160). It is only auto-detected (and ignored) for
  **file sources** (`--tasks`). A **cube read** (`--tm1-instance`) cannot
  infer the mode — every workflow occupies the same cube measures — so it
  defaults to `norm` and silently drops the `predecessors` measure unless
  `--mode opt` is passed. This caused predecessors to disappear from
  cube-read execution plans (e.g. `Sample_Optimal_Mode`). Updated the CLI
  reference, settings reference, migration guide, TM1 integration guide
  (new "Choosing the mode for cube reads" section), getting-started
  task-files page, and the `rushti run --help` text.

## [2.2.3] - 2026-06-01

- Fix: `TM1Service` kwarg collision when connection parameters are set in
  `config.ini` (#158).

## [2.2.2] - 2026-05-20

- Fix: `rushti build` now installs a TM1-version-aware `}rushti.load.results`
  TI (closes #154). On v12 targets the body no longer references the removed
  `CubeGetLogChanges` / `CubeSetLogChanges` / `ExecuteCommand` functions;
  source-file cleanup uses the TM1-native `ASCIIDelete` instead of shelling
  out via `cmd /c del`. The v11 body is unchanged.

## [2.2.1] - 2026-05-20

- **Per-workflow `tm1_instance` setting** for results push and auto-load.
  Set it inside a JSON taskfile's `settings` block to override the
  `settings.ini` default per workflow. Resolution chain (highest wins):
  CLI `--tm1-instance` > taskfile `settings.tm1_instance` >
  `settings.ini [tm1_integration].tm1_instance` >
  `settings.ini [tm1_integration].default_tm1_instance` (deprecated).
  RushTI now logs which tier supplied the target at push time.
- **Behavioural change:** `push_results` and `auto_load_results` set
  inside taskfile JSON now take effect. They were silently ignored in
  the `run` path before this release. If you have divergent values
  between taskfile JSON and `settings.ini`, reconcile them before
  upgrading — the taskfile value will now win.
- **Soft deprecation:** `settings.ini [tm1_integration].default_tm1_instance`.
  Rename to `tm1_instance`. The old key is honoured indefinitely as the
  final fallback in the resolution chain; a one-shot `DEPRECATION:`
  warning fires at settings load only when it's the value actually being
  used.
- **Soft deprecation:** `rushti tasks push --target-tm1-instance`. Use
  `--tm1-instance` instead. The legacy flag is aliased and continues to
  work; a `DEPRECATION:` warning fires on use.

## [2.2.0] - 2026-05-18

- Add `--detailed-results` for per-execution cube rows (closes #146).
- Log migration hint at run start when `--detailed-results` is enabled.
- Fix: `--tm1-instance` CLI argument now overrides `default_tm1_instance` for
  results push and auto-load, not just for taskfile read.
- Fix: parameter parser no longer treats backslash as an escape character
  inside quoted values; Windows-style paths (e.g. `F:\Cons\Go_Files\`) parse
  correctly in both TM1-sourced and text-based taskfiles.
- Change: parameters in pushed result rows are now rendered as inline
  `key="value"` pairs (matching the input cube format) instead of a JSON
  dictionary.
