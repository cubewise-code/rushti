# Changelog

All notable changes to RushTI are documented in this file.

## Unreleased — `feat/issue-156-chore-task-kind`

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

## Unreleased — `feat/issue-154-v12-load-results`

- Fix: `rushti build` now installs a TM1-version-aware `}rushti.load.results`
  TI (closes #154). On v12 targets the body no longer references the removed
  `CubeGetLogChanges` / `CubeSetLogChanges` / `ExecuteCommand` functions;
  source-file cleanup uses the TM1-native `ASCIIDelete` instead of shelling
  out via `cmd /c del`. The v11 body is unchanged.
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

## Unreleased — `feat/issue-146-detailed-results`

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
