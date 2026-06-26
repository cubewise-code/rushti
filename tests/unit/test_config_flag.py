"""Tests for the ``--config`` CLI flag (issue #164).

``--config PATH`` overrides the location of ``config.ini`` (TM1 connection
parameters) for a single invocation, on every TM1-connecting command
(``run``, ``build``, ``tasks …``, ``resume``). It does not exist on
``stats``/``db`` (no TM1 connection).

What we test here (no TM1 required):
- ``add_config_arg`` parses ``--config`` (long-only, default None).
- ``run`` threads the flag through ``parse_arguments`` and fails fast on a
  missing path.
- ``build`` resolves and uses the supplied path (clean exit on a bad path,
  and the resolved path appears in the "instance not found" message).
- each ``tasks`` subcommand resolves the flag and forwards ``config_path``
  to its handler.
- ``resume`` forwards ``--config`` into the rebuilt argv so it survives the
  hand-off to the ``run`` path.

The precedence chain inside ``resolve_config_path`` itself
(``--config`` > ``RUSHTI_DIR`` > legacy CWD > ``config/``) is covered by
``test_config_resolution.py``/``test_cli_dispatch.py``.
"""

import argparse
import sys

import pytest

from rushti import cli
from rushti.app_paths import add_config_arg
from rushti.commands.build import run_build_command
from rushti.commands.resume import run_resume_command
from rushti.commands.tasks import run_tasks_command

# ---------------------------------------------------------------------------
# add_config_arg — the shared helper
# ---------------------------------------------------------------------------


class TestAddConfigArg:
    def test_long_flag_parses_into_config_dest(self):
        parser = argparse.ArgumentParser()
        add_config_arg(parser)
        args = parser.parse_args(["--config", "/some/config.ini"])
        assert args.config == "/some/config.ini"

    def test_default_is_none(self):
        parser = argparse.ArgumentParser()
        add_config_arg(parser)
        args = parser.parse_args([])
        assert args.config is None

    def test_no_short_alias(self):
        # -c must remain free (it's `resume --checkpoint`).
        parser = argparse.ArgumentParser()
        add_config_arg(parser)
        with pytest.raises(SystemExit):
            parser.parse_args(["-c", "/some/config.ini"])


# ---------------------------------------------------------------------------
# run — parse threading + fail-fast
# ---------------------------------------------------------------------------


class TestRunConfigFlag:
    def test_parse_arguments_threads_config(self, tmp_path):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text('{"version": "2.0", "tasks": []}')
        cfg = tmp_path / "config.ini"
        cfg.write_text("[tm1srv01]\n")
        _, cli_args = cli.parse_arguments(
            ["rushti", "--tasks", str(taskfile), "--config", str(cfg)]
        )
        assert cli_args["config"] == str(cfg)

    def test_parse_arguments_config_absent_is_none(self, tmp_path):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text('{"version": "2.0", "tasks": []}')
        _, cli_args = cli.parse_arguments(["rushti", "--tasks", str(taskfile)])
        assert cli_args["config"] is None

    def test_positional_style_has_config_none(self, tmp_path):
        tasks_file = tmp_path / "tasks.txt"
        tasks_file.write_text("instance=tm1srv01 process=}bedrock.server.wait\n")
        _, cli_args = cli.parse_arguments(["rushti", str(tasks_file), "4"])
        assert cli_args["config"] is None

    def test_missing_config_path_exits_clean(self, tmp_path, monkeypatch):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text('{"version": "2.0", "tasks": []}')
        bad = str(tmp_path / "nope.ini")
        monkeypatch.setattr(sys, "argv", ["rushti", "--tasks", str(taskfile), "--config", bad])
        with pytest.raises(SystemExit) as exc_info:
            cli.main()
        # Clean string message (no traceback), and it names the bad path.
        assert isinstance(exc_info.value.code, str)
        assert bad in exc_info.value.code


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


class TestBuildConfigFlag:
    def test_missing_config_path_exits_clean(self, tmp_path):
        bad = str(tmp_path / "nope.ini")
        with pytest.raises(SystemExit) as exc_info:
            run_build_command(["rushti", "build", "--tm1-instance", "tm1srv01", "--config", bad])
        assert isinstance(exc_info.value.code, str)
        assert bad in exc_info.value.code

    def test_resolved_config_is_used(self, tmp_path, capsys):
        # A real config.ini without the requested instance: build must report
        # the supplied --config path, proving it read *our* file.
        cfg = tmp_path / "shared-config.ini"
        cfg.write_text("[tm1srv01]\naddress = localhost\n")
        with pytest.raises(SystemExit):
            run_build_command(
                ["rushti", "build", "--tm1-instance", "MISSING", "--config", str(cfg)]
            )
        out = capsys.readouterr().out
        assert str(cfg) in out
        assert "MISSING" in out


# ---------------------------------------------------------------------------
# tasks — flag present on every subcommand
# ---------------------------------------------------------------------------


TASKS_SUBCOMMANDS = ["export", "push", "expand", "visualize", "validate"]


class TestTasksConfigFlag:
    @pytest.mark.parametrize("subcommand", TASKS_SUBCOMMANDS)
    def test_subcommand_forwards_config_to_handler(self, subcommand, tmp_path, monkeypatch):
        cfg = tmp_path / "config.ini"
        cfg.write_text("[tm1srv01]\n")
        taskfile = tmp_path / "t.json"
        taskfile.write_text('{"version":"2.0","tasks":[]}')
        output = tmp_path / "out.json"

        captured = {}

        def fake_handler(args, config_path):
            captured["config_path"] = config_path

        monkeypatch.setattr(f"rushti.commands.tasks.handle_tasks_{subcommand}", fake_handler)

        argv = ["rushti", "tasks", subcommand, "--tasks", str(taskfile)]
        # export/expand/visualize require --output
        if subcommand in ("export", "expand", "visualize"):
            argv += ["--output", str(output)]
        argv += ["--config", str(cfg)]

        run_tasks_command(argv)
        assert captured["config_path"] == str(cfg)

    def test_missing_config_path_exits_clean(self, tmp_path):
        bad = str(tmp_path / "nope.ini")
        taskfile = tmp_path / "t.json"
        taskfile.write_text('{"version":"2.0","tasks":[]}')
        with pytest.raises(SystemExit) as exc_info:
            run_tasks_command(
                ["rushti", "tasks", "validate", "--tasks", str(taskfile), "--config", bad]
            )
        assert isinstance(exc_info.value.code, str)
        assert bad in exc_info.value.code


# ---------------------------------------------------------------------------
# resume — flag survives the argv rebuild into the run path
# ---------------------------------------------------------------------------


class TestResumeConfigFlag:
    def test_config_forwarded_into_rebuilt_argv(self, tmp_path, monkeypatch):
        from rushti.checkpoint import Checkpoint, save_checkpoint

        taskfile = tmp_path / "tasks.json"
        taskfile.write_text('{"version": "2.0", "tasks": []}')
        cfg = tmp_path / "shared-config.ini"
        cfg.write_text("[tm1srv01]\n")

        checkpoint = Checkpoint.create(
            taskfile_path=str(taskfile),
            workflow="resume-fixture",
            task_ids=["1"],
        )
        checkpoint_file = tmp_path / "checkpoint.json"
        save_checkpoint(checkpoint, str(checkpoint_file))

        monkeypatch.setattr(sys, "argv", ["rushti"])  # restored by monkeypatch
        context = run_resume_command(
            [
                "rushti",
                "resume",
                "--checkpoint",
                str(checkpoint_file),
                "--tasks",
                str(taskfile),
                "--force",
                "--config",
                str(cfg),
            ]
        )

        # run_resume_command sets sys.argv for main() to re-parse.
        assert context["resume"] is True
        assert "--config" in sys.argv
        idx = sys.argv.index("--config")
        assert sys.argv[idx + 1] == str(cfg)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
