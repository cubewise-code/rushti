# Changelog

All notable changes to RushTI are documented in this file.

## Unreleased — `feat/issue-154-v12-load-results`

- Fix: `rushti build` now installs a TM1-version-aware `}rushti.load.results`
  TI (closes #154). On v12 targets the body no longer references the removed
  `CubeGetLogChanges` / `CubeSetLogChanges` / `ExecuteCommand` functions;
  source-file cleanup uses the TM1-native `ASCIIDelete` instead of shelling
  out via `cmd /c del`. The v11 body is unchanged.

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
