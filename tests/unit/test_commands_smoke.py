"""Phase 0 safety-net: full main() invocation smoke tests for non-TM1 subcommands.

These tests prove the full ``argv -> main() -> handler -> output`` chain works
for subcommands that do not require a live TM1 instance:

- ``tasks expand``  (file in/out)
- ``tasks visualize``  (file in/out, HTML)
- ``db list``  (reads stats SQLite via --settings file)

They form a representative pattern. The remaining non-TM1 subcommands
(``tasks validate``, ``stats *``, ``db show/clear/vacuum``) have the same
shape — pass --settings pointing at the test DB, assert exit code + key
output. They are intentionally omitted from this initial Phase 0 file to
keep the safety-net commit small; they can be expanded as Phase 2 surfaces
specific risk areas.

For TM1-requiring subcommands (``run``, ``resume``, ``build``,
``tasks export``, ``tasks push``), see ``tests/integration/test_commands_smoke.py``.

Assertions follow the practical level agreed for Phase 0:
- exit code (0 success, non-zero failure)
- key file outputs exist with non-empty content
- key stdout/stderr tokens appear (e.g., a workflow name we just inserted)

See ``docs/architecture/refactoring-plan.md`` (Phase 0) for context.
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

from rushti import cli


def _invoke_main(argv, capsys):
    """Invoke cli.main() with the given argv, capturing exit code + output."""
    with patch.object(sys, "argv", argv):
        try:
            rc = cli.main()
            if rc is None:
                rc = 0
        except SystemExit as e:
            rc = e.code if e.code is not None else 0
            if isinstance(rc, str):
                rc = 1

    captured = capsys.readouterr()
    return rc, captured.out, captured.err


def _write_minimal_json_taskfile(path: Path) -> Path:
    """Write a small valid JSON taskfile with no MDX expansion required."""
    path.write_text(
        json.dumps(
            {
                "version": "2.0",
                "metadata": {
                    "workflow": "smoke-fixture",
                    "description": "Phase 0 smoke fixture",
                },
                "tasks": [
                    {
                        "id": "task1",
                        "instance": "tm1srv01",
                        "process": "}bedrock.server.wait",
                        "parameters": {"pWaitSec": "1"},
                    },
                    {
                        "id": "task2",
                        "instance": "tm1srv01",
                        "process": "}bedrock.server.wait",
                        "parameters": {"pWaitSec": "2"},
                        "predecessors": ["task1"],
                    },
                ],
            },
            indent=2,
        )
    )
    return path


def _write_settings_pointing_at_db(settings_path: Path, db_path: str) -> Path:
    """Write a minimal settings.ini that points the stats DB at db_path."""
    settings_path.write_text(
        "[stats]\n" "enabled = true\n" "backend = sqlite\n" f"db_path = {db_path}\n"
    )
    return settings_path


# ---------------------------------------------------------------------------
# tasks expand / visualize  (file in/out, no DB)
# ---------------------------------------------------------------------------


class TestTasksExpand:
    def test_expand_minimal_taskfile_writes_output(self, tmp_path, capsys):
        input_file = _write_minimal_json_taskfile(tmp_path / "input.json")
        output_file = tmp_path / "expanded.json"

        rc, _, _ = _invoke_main(
            [
                "rushti",
                "tasks",
                "expand",
                "--tasks",
                str(input_file),
                "--output",
                str(output_file),
            ],
            capsys,
        )
        assert rc == 0
        assert output_file.exists()
        assert output_file.stat().st_size > 0
        data = json.loads(output_file.read_text())
        assert "tasks" in data
        assert len(data["tasks"]) >= 1


class TestTasksVisualize:
    def test_visualize_writes_html_output(self, tmp_path, capsys):
        input_file = _write_minimal_json_taskfile(tmp_path / "input.json")
        output_file = tmp_path / "graph.html"

        rc, _, _ = _invoke_main(
            [
                "rushti",
                "tasks",
                "visualize",
                "--tasks",
                str(input_file),
                "--output",
                str(output_file),
            ],
            capsys,
        )
        assert rc == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "task1" in content or "task2" in content


# ---------------------------------------------------------------------------
# db list  (reads stats SQLite, exercises --settings precedence)
# ---------------------------------------------------------------------------


class TestDbList:
    def test_list_runs_includes_populated_workflow(self, populated_stats_db, tmp_path, capsys):
        settings_file = _write_settings_pointing_at_db(
            tmp_path / "settings.ini", populated_stats_db
        )
        # `db list` requires a sub-sub-command; "workflows" lists known workflows.
        rc, out, err = _invoke_main(
            [
                "rushti",
                "db",
                "list",
                "workflows",
                "--settings",
                str(settings_file),
            ],
            capsys,
        )
        assert rc == 0
        combined = out + err
        # The populated DB has a "smoke-test-workflow" run
        assert "smoke-test-workflow" in combined
