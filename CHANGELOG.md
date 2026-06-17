# Changelog

All notable changes to RushTI are documented in this file.

## Unreleased — docs: `--mode` for cube reads (#160)

- **Docs fix:** corrected the long-standing claim that `--mode` is
  deprecated/ignored. It is only auto-detected (and ignored) for **file
  sources** (`--tasks`). A **cube read** (`--tm1-instance`) cannot infer
  the mode — every workflow occupies the same cube measures — so it
  defaults to `norm` and silently drops the `predecessors` measure unless
  `--mode opt` is passed. This caused predecessors to disappear from
  cube-read execution plans (e.g. `Sample_Optimal_Mode`). Updated the CLI
  reference, settings reference, migration guide, TM1 integration guide
  (new "Choosing the mode for cube reads" section), getting-started
  task-files page, and the `rushti run --help` text.

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
