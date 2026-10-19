"""Tests for run phase time limits (issue #124).

A hang is a run phase going past its time limit. When that happens a
watchdog thread logs which limit was hit, writes the result file with
``Status`` = ``TimedOut``, keeps the checkpoint, logs out of TM1 in the
background (waiting a few seconds at most) and exits with code 1.

The blocking TM1 calls are stubbed with ``Hang.block``, which waits until
the watchdog "exits". ``os._exit`` is stubbed so the test process survives;
the stub takes a snapshot of the files the watchdog left behind, because
after it returns the released call lets ``main()`` carry on.

What we test (no TM1 required):
- Each of the four limits fires: exit code 1, a log line naming the phase,
  ``Status`` = ``TimedOut``, and exit within the limit plus a few seconds.
- ``task_retrieval`` is ignored for local taskfiles.
- ``connection`` ends when the first task starts, and the exclusive-mode
  wait doesn't count against it.
- A ``task_execution`` timeout keeps the checkpoint and writes local stats.
- Logout: sessions opened so far are logged out, preserved connections are
  skipped unless exclusive mode forces them, and a logout that never
  returns doesn't hold up the exit.
"""

import logging
import os
import sqlite3
import sys
import threading
import time
from datetime import datetime

import pytest

from rushti import cli, execution, timeouts, tm1_integration
from rushti.checkpoint import load_checkpoint

LIMIT = 1  # seconds; the smallest limit the settings accept
EXIT_GRACE = 5  # the watchdog's logout wait, plus scheduling slack


class Released(Exception):
    """Raised by a stubbed TM1 call once the watchdog has exited."""


class Hang:
    """Stands in for ``os._exit`` and for TM1 calls that never return."""

    def __init__(self):
        self.exited = threading.Event()
        self.code = None
        self.exited_at = None
        self.snapshot = None
        self.take_snapshot = lambda: None

    def exit(self, code):
        self.code = code
        self.exited_at = time.monotonic()
        self.snapshot = self.take_snapshot()
        self.exited.set()

    def block(self, *args, **kwargs):
        self.exited.wait(30)
        raise Released()


class FakeTM1:
    def __init__(self, logout_blocks_until=None):
        self.logged_out = threading.Event()
        self._logout_blocks_until = logout_blocks_until

    def logout(self):
        if self._logout_blocks_until is not None:
            self._logout_blocks_until.wait(30)
        self.logged_out.set()


def _read_result(path):
    header, record = path.read_text(encoding="utf-8").splitlines()
    return dict(zip(header.split("|"), record.split("|")))


@pytest.fixture
def hang(monkeypatch):
    hang = Hang()
    monkeypatch.setattr(os, "_exit", hang.exit)
    monkeypatch.setattr(logging, "shutdown", lambda: None)
    yield hang
    hang.exited.set()  # release anything still blocked


@pytest.fixture
def env(tmp_path, monkeypatch, caplog):
    """A local run with two tasks (2 depends on 1) and stubbed TM1 access."""
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("RUSHTI_DIR", str(tmp_path))

    class Env:
        root = tmp_path
        taskfile = tmp_path / "tasks.json"
        config = tmp_path / "config.ini"
        settings = tmp_path / "settings.ini"
        result = tmp_path / "result.csv"
        checkpoints = tmp_path / "checkpoints"
        stats_db = tmp_path / "stats.db"
        services = {"tm1srv01": FakeTM1()}
        preserve = {}

        def write_settings(self, body=""):
            self.settings.write_text(
                f"[resume]\nenabled = true\ncheckpoint_dir = {self.checkpoints}\n{body}"
            )

        def args(self, *extra):
            return [
                "--tasks",
                str(self.taskfile),
                "--config",
                str(self.config),
                "--settings",
                str(self.settings),
                "--result",
                str(self.result),
                *extra,
            ]

    e = Env()
    e.taskfile.write_text(
        '{"version": "2.0", "tasks": ['
        '{"id": "1", "instance": "tm1srv01", "process": "p1"},'
        '{"id": "2", "instance": "tm1srv01", "process": "p2", "predecessors": ["1"]}]}'
    )
    e.config.write_text("[tm1srv01]\naddress = localhost\n")
    e.write_settings()

    def _setup(**kwargs):
        # Like the real setup_tm1_services: fill the dicts it's given
        kwargs["tm1_services"].update(e.services)
        kwargs["tm1_preserve_connections"].update(e.preserve)
        return kwargs["tm1_services"], kwargs["tm1_preserve_connections"]

    monkeypatch.setattr(cli, "setup_tm1_services", _setup)
    monkeypatch.setattr(cli, "wait_for_exclusive_access", lambda **kwargs: None)
    monkeypatch.setattr(cli, "validate_tasks", lambda *args: True)
    monkeypatch.setattr(execution, "execute_task", lambda ctx, task, *args: True)
    return e


def _run_main(monkeypatch, args):
    """Run main() the way the TI caller would, and return the elapsed seconds."""
    monkeypatch.setattr(sys, "argv", ["rushti", *args])
    started = time.monotonic()
    try:
        cli.main()
    except (SystemExit, Released):
        pass
    return started


def _assert_timed_out(hang, started, result_file, phase, caplog, runs="0"):
    assert hang.code == 1
    assert hang.exited_at - started < LIMIT + EXIT_GRACE
    result = hang.snapshot["result"]
    assert result["Status"] == "TimedOut"
    assert result["Overall Success"] == "False"
    assert result["Process Runs"] == runs
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert any(phase in m and f"{LIMIT}s" in m for m in errors), errors


def _snapshot_result(env, hang, **extra):
    def take():
        snapshot = {"result": _read_result(env.result)}
        snapshot.update({name: fn() for name, fn in extra.items()})
        return snapshot

    hang.take_snapshot = take


class TestTaskRetrieval:
    def test_limit_fires(self, env, hang, monkeypatch, caplog):
        source = FakeTM1()
        monkeypatch.setattr(tm1_integration, "connect_to_tm1_instance", lambda *a: source)
        monkeypatch.setattr(tm1_integration, "read_taskfile_from_tm1", hang.block)
        _snapshot_result(env, hang, source_logged_out=source.logged_out.is_set)

        started = _run_main(
            monkeypatch,
            [
                "--tm1-instance",
                "tm1srv01",
                "--workflow",
                "wf",
                "--config",
                str(env.config),
                "--settings",
                str(env.settings),
                "--result",
                str(env.result),
                "--timeout-task-retrieval",
                str(LIMIT),
            ],
        )

        _assert_timed_out(hang, started, env.result, "task_retrieval", caplog)
        assert hang.snapshot["source_logged_out"] is True

    def test_ignored_for_local_taskfile(self, env, hang, monkeypatch):
        def _slow_setup(**kwargs):
            time.sleep(LIMIT + 0.5)
            return env.services, env.preserve

        monkeypatch.setattr(cli, "setup_tm1_services", _slow_setup)
        with pytest.raises(SystemExit) as exc_info:
            monkeypatch.setattr(
                sys, "argv", ["rushti", *env.args("--timeout-task-retrieval", str(LIMIT))]
            )
            cli.main()
        assert exc_info.value.code == 0
        assert hang.code is None
        assert _read_result(env.result)["Status"] == "Success"


class TestConnection:
    def test_limit_fires(self, env, hang, monkeypatch, caplog):
        monkeypatch.setattr(cli, "setup_tm1_services", hang.block)
        _snapshot_result(env, hang)

        started = _run_main(monkeypatch, env.args("--timeout-connection", str(LIMIT)))

        _assert_timed_out(hang, started, env.result, "connection", caplog)

    def test_sessions_opened_before_the_hang_are_logged_out(self, env, hang, monkeypatch):
        opened = FakeTM1()

        def _setup(**kwargs):
            kwargs["tm1_services"]["tm1srv01"] = opened
            hang.block()

        monkeypatch.setattr(cli, "setup_tm1_services", _setup)
        _snapshot_result(env, hang, logged_out=opened.logged_out.is_set)

        _run_main(monkeypatch, env.args("--timeout-connection", str(LIMIT)))

        assert hang.code == 1
        assert hang.snapshot["logged_out"] is True

    def test_exclusive_wait_does_not_count(self, env, hang, monkeypatch):
        monkeypatch.setattr(
            cli, "wait_for_exclusive_access", lambda **kwargs: time.sleep(LIMIT + 0.5)
        )
        monkeypatch.setattr(sys, "argv", ["rushti", *env.args("--timeout-connection", str(LIMIT))])
        with pytest.raises(SystemExit) as exc_info:
            cli.main()
        assert exc_info.value.code == 0
        assert hang.code is None
        assert _read_result(env.result)["Status"] == "Success"

    def test_ends_when_first_task_starts(self, env, hang, monkeypatch):
        def _slow_task(ctx, task, *args):
            time.sleep((LIMIT + 0.5) / 2)
            return True

        monkeypatch.setattr(execution, "execute_task", _slow_task)
        monkeypatch.setattr(sys, "argv", ["rushti", *env.args("--timeout-connection", str(LIMIT))])
        with pytest.raises(SystemExit) as exc_info:
            cli.main()
        assert exc_info.value.code == 0
        assert hang.code is None


class TestTaskExecution:
    def _second_task_hangs(self, monkeypatch, hang):
        def _execute(ctx, task, *args):
            if str(task.id) == "2":
                hang.block()
            now = datetime.now()
            execution._collect_task_stats(ctx, task, success=True, start_time=now, end_time=now)
            return True

        monkeypatch.setattr(execution, "execute_task", _execute)

    def test_limit_fires_and_keeps_checkpoint(self, env, hang, monkeypatch, caplog):
        self._second_task_hangs(monkeypatch, hang)
        _snapshot_result(
            env,
            hang,
            checkpoints=lambda: [load_checkpoint(str(f)) for f in env.checkpoints.glob("*.json")],
        )

        started = _run_main(monkeypatch, env.args("--timeout-task-execution", str(LIMIT)))

        _assert_timed_out(hang, started, env.result, "task_execution", caplog, runs="1")
        assert hang.snapshot["result"]["Process Fails"] == "0"
        (checkpoint,) = hang.snapshot["checkpoints"]
        assert "1" in checkpoint.completed_tasks
        assert "2" in checkpoint.in_progress_tasks

    def test_writes_local_stats(self, env, hang, monkeypatch):
        env.write_settings(f"[stats]\nenabled = true\ndb_path = {env.stats_db}\n")
        self._second_task_hangs(monkeypatch, hang)

        def _stats():
            with sqlite3.connect(env.stats_db) as conn:
                status = conn.execute("SELECT status FROM runs").fetchone()[0]
                tasks = conn.execute("SELECT COUNT(*) FROM task_results").fetchone()[0]
            return status, tasks

        _snapshot_result(env, hang, stats=_stats)

        _run_main(monkeypatch, env.args("--timeout-task-execution", str(LIMIT)))

        assert hang.code == 1
        assert hang.snapshot["stats"] == ("Partial", 1)

    def test_logs_out_of_task_instances(self, env, hang, monkeypatch):
        self._second_task_hangs(monkeypatch, hang)
        session = env.services["tm1srv01"]
        _snapshot_result(env, hang, logged_out=session.logged_out.is_set)

        _run_main(monkeypatch, env.args("--timeout-task-execution", str(LIMIT)))

        assert hang.snapshot["logged_out"] is True

    def test_preserved_connection_not_logged_out(self, env, hang, monkeypatch):
        self._second_task_hangs(monkeypatch, hang)
        env.preserve["tm1srv01"] = True
        session = env.services["tm1srv01"]
        _snapshot_result(env, hang, logged_out=session.logged_out.is_set)

        _run_main(monkeypatch, env.args("--timeout-task-execution", str(LIMIT)))

        assert hang.code == 1
        assert hang.snapshot["logged_out"] is False

    def test_exclusive_mode_logs_out_preserved_connection(self, env, hang, monkeypatch):
        self._second_task_hangs(monkeypatch, hang)
        env.preserve["tm1srv01"] = True
        session = env.services["tm1srv01"]
        _snapshot_result(env, hang, logged_out=session.logged_out.is_set)

        _run_main(monkeypatch, env.args("--exclusive", "--timeout-task-execution", str(LIMIT)))

        assert hang.code == 1
        assert hang.snapshot["logged_out"] is True

    def test_logout_that_never_returns_does_not_hold_up_exit(self, env, hang, monkeypatch, caplog):
        monkeypatch.setattr(timeouts, "LOGOUT_WAIT_SECONDS", 1)
        self._second_task_hangs(monkeypatch, hang)
        env.services["tm1srv01"] = FakeTM1(logout_blocks_until=hang.exited)
        _snapshot_result(env, hang)

        started = _run_main(monkeypatch, env.args("--timeout-task-execution", str(LIMIT)))

        assert hang.code == 1
        assert hang.exited_at - started < LIMIT + timeouts.LOGOUT_WAIT_SECONDS + 1
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any("tm1srv01" in m and "logout" in m.lower() for m in warnings), warnings

    def test_final_logout_counts_against_task_execution(self, env, hang, monkeypatch, caplog):
        monkeypatch.setattr(timeouts, "LOGOUT_WAIT_SECONDS", 1)

        class EarlyReleaseFails(FakeTM1):
            """Early release fails, so the end-of-run logout retries it, and that one blocks."""

            calls = 0

            def logout(self):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("early release failed")
                hang.block()

        env.services["tm1srv01"] = EarlyReleaseFails()
        _snapshot_result(env, hang)

        started = _run_main(monkeypatch, env.args("--timeout-task-execution", str(LIMIT)))

        _assert_timed_out(hang, started, env.result, "task_execution", caplog, runs="2")


class TestPhaseWatchdog:
    """A hang that's already being handled must finish before the run goes on."""

    @pytest.mark.parametrize("next_step", ["stop", "start"])
    def test_waits_for_a_hang_in_progress(self, next_step):
        in_handler, release = threading.Event(), threading.Event()

        def on_hang(phase, limit):
            in_handler.set()
            release.wait(5)

        watchdog = timeouts.PhaseWatchdog(on_hang)
        watchdog.start("connection", 0.1)
        assert in_handler.wait(2)

        returned = threading.Event()

        def _next():
            if next_step == "stop":
                watchdog.stop()
            else:
                watchdog.start("task_execution", 0)
            returned.set()

        threading.Thread(target=_next, daemon=True).start()
        assert not returned.wait(0.3)  # blocked while the hang is handled
        release.set()
        assert returned.wait(2)

    def test_stop_before_the_limit_means_no_hang(self):
        hangs = []
        watchdog = timeouts.PhaseWatchdog(lambda phase, limit: hangs.append(phase))
        watchdog.start("connection", 0.2)
        watchdog.stop()
        time.sleep(0.4)
        assert hangs == []

    def test_paused_time_does_not_count(self):
        hangs = []
        watchdog = timeouts.PhaseWatchdog(lambda phase, limit: hangs.append(phase))
        watchdog.start("connection", 0.3)
        with watchdog.paused():
            time.sleep(0.5)
        assert hangs == []
        time.sleep(0.5)
        assert hangs == ["connection"]


class TestPushResults:
    def test_limit_fires(self, env, hang, monkeypatch, caplog):
        env.write_settings(
            f"[stats]\nenabled = true\ndb_path = {env.stats_db}\n"
            "[tm1_integration]\npush_results = true\ntm1_instance = tm1srv01\n"
        )
        monkeypatch.setattr(tm1_integration, "connect_to_tm1_instance", hang.block)
        _snapshot_result(env, hang)

        started = _run_main(monkeypatch, env.args("--timeout-push-results", str(LIMIT)))

        _assert_timed_out(hang, started, env.result, "push_results", caplog, runs="2")


class TestNoLimits:
    def test_slow_phases_run_to_completion(self, env, hang, monkeypatch):
        def _slow_task(ctx, task, *args):
            time.sleep(0.2)
            return True

        monkeypatch.setattr(execution, "execute_task", _slow_task)
        monkeypatch.setattr(sys, "argv", ["rushti", *env.args()])
        with pytest.raises(SystemExit) as exc_info:
            cli.main()
        assert exc_info.value.code == 0
        assert hang.code is None
        assert not list(env.checkpoints.glob("*.json"))  # cleaned up on success
