"""Tests for the ``--result`` summary file on every run outcome (issue #124).

A calling TI process reads the ``--result`` file after RushTI returns to
decide what to do next. If RushTI aborts without writing it, the caller
can't tell "aborted" from "never ran", so every exit path of a run must
write the file with ``Overall Success`` = ``False``.

What we test (no TM1 required):
- ``exit_rushti`` writes the file on an aborted run, not only on completion.
- ``main()`` writes the file when it exits early: missing ``--config``,
  invalid taskfile, and exclusive-mode timeout.
- The ``Status`` column, last in the row, for each outcome: ``Success``,
  ``Failed``, ``Aborted`` (``TimedOut`` is covered in
  ``test_run_phase_timeouts.py``).
"""

import sys
from datetime import datetime

import pytest

from rushti import cli
from rushti.exclusive import ExclusiveModeTimeoutError


def _read_result(path):
    header, record = path.read_text(encoding="utf-8").splitlines()
    return dict(zip(header.split("|"), record.split("|")))


class TestExitRushti:
    def test_aborted_run_writes_result_file(self, tmp_path):
        result_file = tmp_path / "result.csv"
        start = datetime.now()
        with pytest.raises(SystemExit) as exc_info:
            cli.exit_rushti(
                overall_success=False,
                executions=3,
                successes=1,
                start_time=start,
                end_time=start,
                elapsed_time=start - start,
                result_file=str(result_file),
            )
        assert exc_info.value.code != 0
        result = _read_result(result_file)
        assert result["Overall Success"] == "False"
        assert result["Process Runs"] == "3"
        assert result["Process Fails"] == "2"
        assert result["Status"] == "Aborted"

    def test_completed_run_still_writes_result_file(self, tmp_path):
        result_file = tmp_path / "result.csv"
        start = datetime.now()
        with pytest.raises(SystemExit) as exc_info:
            cli.exit_rushti(
                overall_success=True,
                executions=2,
                successes=2,
                start_time=start,
                end_time=start,
                elapsed_time=start - start,
                result_file=str(result_file),
            )
        assert exc_info.value.code == 0
        result = _read_result(result_file)
        assert result["Overall Success"] == "True"
        assert result["Status"] == "Success"

    def test_run_with_failed_tasks_reports_failed(self, tmp_path):
        result_file = tmp_path / "result.csv"
        start = datetime.now()
        with pytest.raises(SystemExit) as exc_info:
            cli.exit_rushti(
                overall_success=True,
                executions=3,
                successes=2,
                start_time=start,
                end_time=start,
                elapsed_time=start - start,
                result_file=str(result_file),
            )
        assert exc_info.value.code not in (0, None)
        result = _read_result(result_file)
        assert result["Overall Success"] == "True"
        assert result["Process Fails"] == "1"
        assert result["Status"] == "Failed"

    def test_status_is_the_last_column(self, tmp_path):
        result_file = tmp_path / "result.csv"
        start = datetime.now()
        with pytest.raises(SystemExit):
            cli.exit_rushti(
                overall_success=True,
                executions=1,
                successes=1,
                start_time=start,
                end_time=start,
                elapsed_time=start - start,
                result_file=str(result_file),
            )
        header = result_file.read_text(encoding="utf-8").splitlines()[0].split("|")
        assert header == [
            "PID",
            "Process Runs",
            "Process Fails",
            "Start",
            "End",
            "Runtime",
            "Overall Success",
            "Status",
        ]


class TestEarlyExitWritesResultFile:
    def _run_main(self, monkeypatch, *args):
        monkeypatch.setattr(sys, "argv", ["rushti", *args])
        with pytest.raises(SystemExit) as exc_info:
            cli.main()
        assert exc_info.value.code not in (0, None)

    def _assert_aborted(self, result_file):
        result = _read_result(result_file)
        assert result["Overall Success"] == "False"
        assert result["Process Runs"] == "0"
        assert result["Status"] == "Aborted"

    def test_missing_config(self, tmp_path, monkeypatch):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text('{"version": "2.0", "tasks": []}')
        result_file = tmp_path / "result.csv"
        self._run_main(
            monkeypatch,
            "--tasks",
            str(taskfile),
            "--config",
            str(tmp_path / "nope.ini"),
            "--result",
            str(result_file),
        )
        self._assert_aborted(result_file)

    def test_invalid_taskfile(self, tmp_path, monkeypatch):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text('{"version": "2.0", "tasks": [{"id": "1"}]}')
        config = tmp_path / "config.ini"
        config.write_text("[tm1srv01]\naddress = localhost\n")
        result_file = tmp_path / "result.csv"
        self._run_main(
            monkeypatch,
            "--tasks",
            str(taskfile),
            "--config",
            str(config),
            "--result",
            str(result_file),
        )
        self._assert_aborted(result_file)

    def test_exclusive_mode_timeout(self, tmp_path, monkeypatch):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text(
            '{"version": "2.0", "tasks": '
            '[{"id": "1", "instance": "tm1srv01", "process": "}bedrock.server.wait"}]}'
        )
        config = tmp_path / "config.ini"
        config.write_text("[tm1srv01]\naddress = localhost\n")
        result_file = tmp_path / "result.csv"

        def _timeout(**kwargs):
            raise ExclusiveModeTimeoutError("timed out")

        monkeypatch.setattr(cli, "setup_tm1_services", lambda **kwargs: ({}, {}))
        monkeypatch.setattr(cli, "wait_for_exclusive_access", _timeout)
        monkeypatch.setattr(cli, "logout", lambda *args, **kwargs: None)
        self._run_main(
            monkeypatch,
            "--tasks",
            str(taskfile),
            "--config",
            str(config),
            "--result",
            str(result_file),
        )
        self._assert_aborted(result_file)
