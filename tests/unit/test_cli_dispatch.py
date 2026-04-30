"""Phase 0 safety-net: CLI argument parsing and subcommand dispatch.

These tests pin the current behavior of ``rushti.cli`` so the Phase 1
extraction (moving ``resolve_config_path``, ``apply_log_level``,
``create_results_file`` etc. out of cli.py) cannot silently regress
argument parsing or dispatch routing.

What we test (no TM1 required):
- ``uses_named_arguments`` style detection
- ``translate_cmd_arguments`` legacy positional -> named conversion
- ``parse_arguments`` dispatcher for both styles
- ``resolve_config_path`` lookup precedence (CLI flag > RUSHTI_DIR > legacy CWD > config/)
- ``main()`` --help / --version / bad subcommand exit codes
- ``main()`` subcommand dispatch routes to the correct handler

What we do NOT test here (covered by integration smoke tests):
- Full execution of any subcommand handler
- TM1 connection setup
- File-output behavior of subcommands

See ``docs/architecture/refactoring-plan.md`` (Phase 0) for context.
"""

import sys
from unittest.mock import patch

import pytest

from rushti import cli

# ---------------------------------------------------------------------------
# uses_named_arguments
# ---------------------------------------------------------------------------


class TestUsesNamedArguments:
    def test_named_long_flag_detected(self):
        assert cli.uses_named_arguments(["rushti", "--tasks-file", "x.txt"]) is True

    def test_short_flag_detected(self):
        assert cli.uses_named_arguments(["rushti", "-t", "x.txt"]) is True

    def test_pure_positional_not_detected(self):
        assert cli.uses_named_arguments(["rushti", "tasks.txt", "4"]) is False

    def test_subcommand_only_not_named(self):
        # Subcommands like "build" should not trip the named-args detection;
        # subcommand dispatch happens before parse_arguments runs.
        assert cli.uses_named_arguments(["rushti", "build"]) is False


# ---------------------------------------------------------------------------
# translate_cmd_arguments (legacy positional)
# ---------------------------------------------------------------------------


class TestTranslateCmdArguments:
    def test_minimal_positional_args(self, tmp_path):
        tasks_file = tmp_path / "tasks.txt"
        tasks_file.write_text("instance=tm1srv01 process=}bedrock.server.wait\n")
        result = cli.translate_cmd_arguments("rushti", str(tasks_file), "4")
        tasks_file_out, max_workers, mode, retries, result_file = result
        assert tasks_file_out == str(tasks_file)
        assert max_workers == 4

    def test_with_mode_and_retries(self, tmp_path):
        tasks_file = tmp_path / "tasks.txt"
        tasks_file.write_text("instance=tm1srv01 process=}bedrock.server.wait\n")
        result = cli.translate_cmd_arguments("rushti", str(tasks_file), "4", "norm", "2")
        tasks_file_out, max_workers, mode, retries, result_file = result
        assert tasks_file_out == str(tasks_file)
        assert max_workers == 4
        assert retries == 2

    def test_invalid_max_workers_exits(self, tmp_path):
        tasks_file = tmp_path / "tasks.txt"
        tasks_file.write_text("instance=tm1srv01\n")
        with pytest.raises(SystemExit):
            cli.translate_cmd_arguments("rushti", str(tasks_file), "not-a-number")

    def test_missing_tasks_file_exits(self, tmp_path):
        with pytest.raises(SystemExit):
            cli.translate_cmd_arguments("rushti", str(tmp_path / "does-not-exist.txt"), "4")


# ---------------------------------------------------------------------------
# parse_arguments dispatcher
# ---------------------------------------------------------------------------


class TestParseArguments:
    def test_positional_style_returns_legacy_dict(self, tmp_path):
        tasks_file = tmp_path / "tasks.txt"
        tasks_file.write_text("instance=tm1srv01 process=}bedrock.server.wait\n")
        path, cli_args = cli.parse_arguments(["rushti", str(tasks_file), "8"])
        assert path == str(tasks_file)
        assert cli_args["max_workers"] == 8
        # log_level not supported in positional style
        assert cli_args["log_level"] is None

    def test_named_style_with_long_flags(self, tmp_path):
        # The named flag is --tasks (not --tasks-file). Stable surface.
        tasks_file = tmp_path / "tasks.json"
        tasks_file.write_text('{"version": "2.0", "tasks": []}')
        path, cli_args = cli.parse_arguments(
            ["rushti", "--tasks", str(tasks_file), "--max-workers", "12"]
        )
        assert path == str(tasks_file)
        assert cli_args["max_workers"] == 12


# ---------------------------------------------------------------------------
# resolve_config_path lookup precedence
# ---------------------------------------------------------------------------


class TestResolveConfigPath:
    """``resolve_config_path`` reads ``cli.CURRENT_DIRECTORY`` (captured at
    module import) for legacy/config-subdir lookups. Tests must patch that
    constant rather than ``os.getcwd()`` / ``monkeypatch.chdir``."""

    def test_cli_path_takes_precedence(self, tmp_path, monkeypatch):
        cli_config = tmp_path / "explicit.ini"
        cli_config.write_text("[tm1srv01]\n")
        # Even with RUSHTI_DIR set, the explicit CLI path wins.
        monkeypatch.setenv("RUSHTI_DIR", str(tmp_path))
        result = cli.resolve_config_path(
            "config.ini", warn_on_legacy=False, cli_path=str(cli_config)
        )
        assert result == str(cli_config)

    def test_cli_path_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            cli.resolve_config_path(
                "config.ini",
                warn_on_legacy=False,
                cli_path=str(tmp_path / "does-not-exist.ini"),
            )

    def test_rushti_dir_env_used(self, tmp_path, monkeypatch):
        config_subdir = tmp_path / "config"
        config_subdir.mkdir()
        target = config_subdir / "config.ini"
        target.write_text("[tm1srv01]\n")
        monkeypatch.setenv("RUSHTI_DIR", str(tmp_path))
        # Point CURRENT_DIRECTORY somewhere with no config/ so the env var wins.
        empty = tmp_path / "elsewhere"
        empty.mkdir()
        monkeypatch.setattr(cli, "CURRENT_DIRECTORY", str(empty))
        result = cli.resolve_config_path("config.ini", warn_on_legacy=False)
        assert result == str(target)

    def test_default_config_subdir_used_when_no_env(self, tmp_path, monkeypatch):
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        target = config_dir / "config.ini"
        target.write_text("[tm1srv01]\n")
        monkeypatch.delenv("RUSHTI_DIR", raising=False)
        monkeypatch.setattr(cli, "CURRENT_DIRECTORY", str(tmp_path))
        result = cli.resolve_config_path("config.ini", warn_on_legacy=False)
        assert result == str(target)

    def test_legacy_cwd_location_used_with_warning(self, tmp_path, monkeypatch):
        legacy_target = tmp_path / "config.ini"
        legacy_target.write_text("[tm1srv01]\n")
        monkeypatch.delenv("RUSHTI_DIR", raising=False)
        monkeypatch.setattr(cli, "CURRENT_DIRECTORY", str(tmp_path))
        # Reset the warning-tracking set so we can observe the side effect.
        monkeypatch.setattr(cli, "_legacy_path_warnings", set())
        result = cli.resolve_config_path("config.ini", warn_on_legacy=True)
        assert result == str(legacy_target)
        # Legacy path was recorded for later deprecation warning.
        assert "config.ini" in cli._legacy_path_warnings


# ---------------------------------------------------------------------------
# main() — exit-code paths and subcommand dispatch
# ---------------------------------------------------------------------------


class TestMainHelpAndVersion:
    def test_top_level_help_exits_zero(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["rushti", "--help"])
        rc = cli.main()
        assert rc == 0
        captured = capsys.readouterr()
        # Banner prints something; help text mentions usage hints
        assert "rushti" in (captured.out + captured.err).lower()

    def test_short_help_flag_exits_zero(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["rushti", "-h"])
        rc = cli.main()
        assert rc == 0

    def test_version_flag_exits_zero(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["rushti", "--version"])
        rc = cli.main()
        assert rc == 0
        captured = capsys.readouterr()
        # Some form of the app name should appear
        assert "rushti" in captured.out.lower()


class TestMainSubcommandDispatch:
    """Each subcommand handler should be invoked exactly once with sys.argv."""

    @pytest.mark.parametrize(
        "subcommand,handler_name",
        [
            ("build", "run_build_command"),
            ("tasks", "run_tasks_command"),
            ("stats", "run_stats_command"),
            ("db", "run_db_command"),
        ],
    )
    def test_subcommand_routes_to_handler(self, monkeypatch, subcommand, handler_name):
        """main() routes the subcommand to its handler. We patch the handler
        in the rushti.cli namespace (where main() looks them up) so the real
        handler does not run."""
        monkeypatch.setattr(sys, "argv", ["rushti", subcommand, "--help"])
        with patch.object(cli, handler_name) as mock_handler:
            mock_handler.return_value = None
            rc = cli.main()
            mock_handler.assert_called_once()
            # main returns 0 for all dispatched subcommands except 'resume'
            assert rc == 0

    def test_resume_subcommand_returns_context_or_exits(self, monkeypatch):
        """The resume handler may return a context dict that main() merges,
        or it may exit early. We only assert main() does not crash."""
        monkeypatch.setattr(sys, "argv", ["rushti", "resume", "--help"])
        with patch.object(cli, "run_resume_command") as mock_resume:
            # Simulate the handler exiting cleanly via SystemExit
            mock_resume.side_effect = SystemExit(0)
            with pytest.raises(SystemExit) as exc_info:
                cli.main()
            assert exc_info.value.code == 0


class TestMainBadInputs:
    def test_missing_tasks_file_exits_with_error(self, monkeypatch):
        """Default 'run' mode without a tasks file should not silently succeed."""
        monkeypatch.setattr(sys, "argv", ["rushti"])
        with pytest.raises(SystemExit):
            cli.main()

    def test_unknown_named_flag_exits(self, monkeypatch):
        """argparse rejects unknown flags with SystemExit(2)."""
        monkeypatch.setattr(sys, "argv", ["rushti", "--this-flag-does-not-exist", "value"])
        with pytest.raises(SystemExit):
            cli.main()
