"""Tests for the run phase time limit settings (issue #124).

Each run phase (``task_retrieval``, ``connection``, ``task_execution``,
``push_results``) has an optional time limit in seconds. ``0`` means no
limit, and that's the default. The limits follow the usual precedence:
CLI > taskfile ``settings.timeouts`` > ``settings.ini [timeouts]`` > default.

What we test (no TM1 required):
- Built-in default is no limit for every phase.
- Precedence for each limit across the three tiers.
- Validation errors for bad values in ``settings.ini``, the taskfile and the CLI.
- ``settings.timeouts`` survives the taskfile and archive round-trips.
- ``rushti resume`` forwards the CLI flags into the rebuilt run argv.
"""

import json
import sys

import pytest

from rushti import cli
from rushti.checkpoint import Checkpoint, save_checkpoint
from rushti.commands.resume import run_resume_command
from rushti.settings import Settings, get_effective_settings, load_settings
from rushti.taskfile import Taskfile, TaskfileSettings, archive_taskfile, validate_taskfile

PHASES = ["task_retrieval", "connection", "task_execution", "push_results"]


def _flag(phase):
    return "--timeout-" + phase.replace("_", "-")


def _write_ini(tmp_path, body):
    path = tmp_path / "settings.ini"
    path.write_text(body)
    return str(path)


def _taskfile_dict(timeouts):
    return {
        "version": "2.0",
        "settings": {"timeouts": timeouts},
        "tasks": [{"id": "1", "instance": "tm1srv01", "process": "}bedrock.server.wait"}],
    }


class TestDefaults:
    @pytest.mark.parametrize("phase", PHASES)
    def test_no_limit_by_default(self, phase):
        assert getattr(Settings().timeouts, phase) == 0


@pytest.mark.parametrize("phase", PHASES)
class TestPrecedence:
    def test_settings_ini(self, tmp_path, phase):
        settings = load_settings(_write_ini(tmp_path, f"[timeouts]\n{phase} = 60\n"))
        assert getattr(settings.timeouts, phase) == 60

    def test_taskfile_overrides_settings_ini(self, tmp_path, phase):
        settings = load_settings(_write_ini(tmp_path, f"[timeouts]\n{phase} = 60\n"))
        settings = get_effective_settings(settings, json_settings={"timeouts": {phase: 30}})
        assert getattr(settings.timeouts, phase) == 30

    def test_taskfile_zero_removes_settings_ini_limit(self, tmp_path, phase):
        settings = load_settings(_write_ini(tmp_path, f"[timeouts]\n{phase} = 60\n"))
        settings = get_effective_settings(settings, json_settings={"timeouts": {phase: 0}})
        assert getattr(settings.timeouts, phase) == 0

    def test_taskfile_leaves_other_phases_alone(self, tmp_path, phase):
        others = [p for p in PHASES if p != phase]
        ini = "[timeouts]\n" + "".join(f"{p} = 60\n" for p in PHASES)
        settings = load_settings(_write_ini(tmp_path, ini))
        settings = get_effective_settings(settings, json_settings={"timeouts": {phase: 30}})
        assert [getattr(settings.timeouts, p) for p in others] == [60] * len(others)

    def test_cli_overrides_taskfile(self, tmp_path, phase):
        settings = load_settings(_write_ini(tmp_path, f"[timeouts]\n{phase} = 60\n"))
        settings = get_effective_settings(
            settings,
            json_settings={"timeouts": {phase: 30}},
            cli_args={f"timeout_{phase}": 10},
        )
        assert getattr(settings.timeouts, phase) == 10

    def test_cli_zero_removes_lower_tier_limits(self, tmp_path, phase):
        settings = load_settings(_write_ini(tmp_path, f"[timeouts]\n{phase} = 60\n"))
        settings = get_effective_settings(
            settings,
            json_settings={"timeouts": {phase: 30}},
            cli_args={f"timeout_{phase}": 0},
        )
        assert getattr(settings.timeouts, phase) == 0

    def test_cli_unset_does_not_override(self, tmp_path, phase):
        settings = load_settings(_write_ini(tmp_path, f"[timeouts]\n{phase} = 60\n"))
        settings = get_effective_settings(settings, cli_args={f"timeout_{phase}": None})
        assert getattr(settings.timeouts, phase) == 60

    def test_cli_flag_parsed(self, tmp_path, phase):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text(json.dumps(_taskfile_dict({})))
        _, cli_args = cli.parse_named_arguments(
            ["rushti", "--tasks", str(taskfile), _flag(phase), "45"]
        )
        assert cli_args[f"timeout_{phase}"] == 45

    def test_cli_flag_absent_is_none(self, tmp_path, phase):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text(json.dumps(_taskfile_dict({})))
        _, cli_args = cli.parse_named_arguments(["rushti", "--tasks", str(taskfile)])
        assert cli_args[f"timeout_{phase}"] is None


class TestSettingsIniValidation:
    def test_empty_value_means_no_limit(self, tmp_path):
        settings = load_settings(_write_ini(tmp_path, "[timeouts]\nconnection =\n"))
        assert settings.timeouts.connection == 0

    @pytest.mark.parametrize("value", ["-1", "abc", "1.5"])
    def test_bad_value_is_an_error(self, tmp_path, value):
        with pytest.raises(ValueError, match=r"\[timeouts\]\.connection"):
            load_settings(_write_ini(tmp_path, f"[timeouts]\nconnection = {value}\n"))


class TestTaskfileValidation:
    def test_valid_timeouts(self):
        timeouts = {phase: 60 for phase in PHASES}
        assert validate_taskfile(_taskfile_dict(timeouts)) == []

    def test_must_be_an_object(self):
        errors = validate_taskfile(_taskfile_dict(60))
        assert any("settings.timeouts" in e for e in errors)

    def test_unknown_key(self):
        errors = validate_taskfile(_taskfile_dict({"connection_timeout": 60}))
        assert any("connection_timeout" in e for e in errors)

    @pytest.mark.parametrize("value", [-1, 1.5, "60", True, None])
    def test_bad_value(self, value):
        errors = validate_taskfile(_taskfile_dict({"connection": value}))
        assert any("settings.timeouts.connection" in e for e in errors)


class TestCliValidation:
    @pytest.mark.parametrize("value", ["-1", "abc", "1.5"])
    def test_bad_value_is_an_error(self, tmp_path, value):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text(json.dumps(_taskfile_dict({})))
        with pytest.raises(SystemExit) as exc_info:
            cli.parse_named_arguments(
                ["rushti", "--tasks", str(taskfile), "--timeout-connection", value]
            )
        assert exc_info.value.code == 2


class TestTaskfileRoundTrip:
    def test_to_dict_from_dict(self):
        settings = TaskfileSettings.from_dict({"timeouts": {"connection": 60, "push_results": 30}})
        assert settings.to_dict()["timeouts"] == {"connection": 60, "push_results": 30}

    def test_absent_timeouts_not_written(self):
        assert "timeouts" not in TaskfileSettings.from_dict({"max_workers": 2}).to_dict()

    def test_archive_round_trip(self, tmp_path, monkeypatch):
        monkeypatch.setenv("RUSHTI_DIR", str(tmp_path))
        taskfile = Taskfile.from_dict(_taskfile_dict({"task_execution": 3600}))
        archived = archive_taskfile(taskfile, "wf", "run1")
        with open(archived) as f:
            data = json.load(f)
        assert data["settings"]["timeouts"] == {"task_execution": 3600}
        assert Taskfile.from_dict(data).settings.timeouts == {"task_execution": 3600}


class TestResumeForwardsTimeouts:
    def test_flags_forwarded_into_rebuilt_argv(self, tmp_path, monkeypatch):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text('{"version": "2.0", "tasks": []}')
        checkpoint = Checkpoint.create(
            taskfile_path=str(taskfile), workflow="resume-fixture", task_ids=["1"]
        )
        checkpoint_file = tmp_path / "checkpoint.json"
        save_checkpoint(checkpoint, str(checkpoint_file))

        monkeypatch.setattr(sys, "argv", ["rushti"])
        argv = ["rushti", "resume", "--checkpoint", str(checkpoint_file), "--tasks", str(taskfile)]
        argv += ["--force"]
        for seconds, phase in enumerate(PHASES):
            argv += [_flag(phase), str(seconds)]  # 0 included: it must be forwarded too
        run_resume_command(argv)

        for seconds, phase in enumerate(PHASES):
            idx = sys.argv.index(_flag(phase))
            assert sys.argv[idx + 1] == str(seconds)

    def test_unset_flags_not_forwarded(self, tmp_path, monkeypatch):
        taskfile = tmp_path / "tasks.json"
        taskfile.write_text('{"version": "2.0", "tasks": []}')
        checkpoint = Checkpoint.create(
            taskfile_path=str(taskfile), workflow="resume-fixture", task_ids=["1"]
        )
        checkpoint_file = tmp_path / "checkpoint.json"
        save_checkpoint(checkpoint, str(checkpoint_file))

        monkeypatch.setattr(sys, "argv", ["rushti"])
        run_resume_command(
            [
                "rushti",
                "resume",
                "--checkpoint",
                str(checkpoint_file),
                "--tasks",
                str(taskfile),
                "--force",
            ]
        )
        assert not any(arg.startswith("--timeout-") for arg in sys.argv)
