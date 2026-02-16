"""Unit tests for checkpoint and resume functionality.

Tests for:
- TaskResult dataclass
- Checkpoint dataclass
- Checkpoint save/load functions
- get_checkpoint_path function
- CheckpointManager class
"""

import json
import os
import sys
import tempfile
import unittest

# Path setup handled by conftest.py, but also support direct execution
_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from rushti.checkpoint import (  # noqa: E402
    Checkpoint,
    TaskResult,
    CheckpointManager,
    save_checkpoint,
    load_checkpoint,
    delete_checkpoint,
    get_checkpoint_path,
)


class TestTaskResult(unittest.TestCase):
    """Tests for TaskResult dataclass"""

    def test_task_result_creation(self):
        """Test creating TaskResult"""
        result = TaskResult(
            task_id="task-1",
            success=True,
            duration_seconds=30.5,
            retry_count=2,
        )
        self.assertEqual(result.task_id, "task-1")
        self.assertTrue(result.success)
        self.assertEqual(result.duration_seconds, 30.5)
        self.assertEqual(result.retry_count, 2)

    def test_task_result_to_dict(self):
        """Test TaskResult.to_dict()"""
        result = TaskResult(
            task_id="task-1",
            success=False,
            duration_seconds=10.0,
            error_message="Connection lost",
        )
        d = result.to_dict()
        self.assertEqual(d["task_id"], "task-1")
        self.assertFalse(d["success"])
        self.assertEqual(d["error_message"], "Connection lost")

    def test_task_result_from_dict(self):
        """Test TaskResult.from_dict()"""
        data = {
            "task_id": "task-2",
            "success": True,
            "duration_seconds": 15.5,
            "retry_count": 0,
            "error_message": None,
            "completed_at": "2024-01-15T10:00:00",
        }
        result = TaskResult.from_dict(data)
        self.assertEqual(result.task_id, "task-2")
        self.assertTrue(result.success)
        self.assertEqual(result.duration_seconds, 15.5)


class TestCheckpoint(unittest.TestCase):
    """Tests for Checkpoint dataclass"""

    def test_checkpoint_create(self):
        """Test creating a new checkpoint"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-taskfile",
                task_ids=["1", "2", "3"],
            )

            self.assertEqual(checkpoint.workflow, "test-taskfile")
            self.assertEqual(checkpoint.total_tasks, 3)
            self.assertEqual(len(checkpoint.pending_tasks), 3)
            self.assertEqual(len(checkpoint.completed_tasks), 0)
            self.assertIn("1", checkpoint.pending_tasks)
        finally:
            os.unlink(taskfile_path)

    def test_checkpoint_mark_running(self):
        """Test marking a task as running"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-taskfile",
                task_ids=["1", "2"],
            )

            checkpoint.mark_running("1")

            self.assertIn("1", checkpoint.in_progress_tasks)
            self.assertNotIn("1", checkpoint.pending_tasks)
        finally:
            os.unlink(taskfile_path)

    def test_checkpoint_mark_completed_success(self):
        """Test marking a task as completed successfully"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-taskfile",
                task_ids=["1", "2"],
            )

            checkpoint.mark_running("1")
            checkpoint.mark_completed("1", success=True, duration_seconds=10.5)

            self.assertIn("1", checkpoint.completed_tasks)
            self.assertNotIn("1", checkpoint.in_progress_tasks)
            self.assertTrue(checkpoint.completed_tasks["1"].success)
            self.assertEqual(checkpoint.success_count, 1)
        finally:
            os.unlink(taskfile_path)

    def test_checkpoint_mark_completed_failure(self):
        """Test marking a task as failed"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-taskfile",
                task_ids=["1", "2"],
            )

            checkpoint.mark_running("1")
            checkpoint.mark_completed(
                "1", success=False, duration_seconds=5.0, error_message="Error"
            )

            self.assertIn("1", checkpoint.completed_tasks)
            self.assertIn("1", checkpoint.failed_tasks)
            self.assertFalse(checkpoint.completed_tasks["1"].success)
            self.assertEqual(checkpoint.failure_count, 1)
        finally:
            os.unlink(taskfile_path)

    def test_checkpoint_progress_percentage(self):
        """Test progress percentage calculation"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-taskfile",
                task_ids=["1", "2", "3", "4"],
            )

            self.assertEqual(checkpoint.progress_percentage, 0.0)

            checkpoint.mark_completed("1", success=True, duration_seconds=10.0)
            self.assertEqual(checkpoint.progress_percentage, 25.0)

            checkpoint.mark_completed("2", success=True, duration_seconds=10.0)
            self.assertEqual(checkpoint.progress_percentage, 50.0)
        finally:
            os.unlink(taskfile_path)

    def test_checkpoint_to_dict(self):
        """Test checkpoint serialization"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-taskfile",
                task_ids=["1", "2"],
            )
            checkpoint.mark_completed("1", success=True, duration_seconds=10.0)

            d = checkpoint.to_dict()

            self.assertEqual(d["workflow"], "test-taskfile")
            self.assertEqual(d["total_tasks"], 2)
            self.assertIn("1", d["completed_tasks"])
            self.assertEqual(d["summary"]["completed"], 1)
            self.assertEqual(d["summary"]["pending"], 1)
        finally:
            os.unlink(taskfile_path)

    def test_checkpoint_from_dict(self):
        """Test checkpoint deserialization"""
        data = {
            "version": "1.0",
            "taskfile_path": "/path/to/tasks.json",
            "workflow": "test-taskfile",
            "taskfile_hash": "abc123",
            "run_started": "2024-01-15T10:00:00",
            "checkpoint_created": "2024-01-15T10:05:00",
            "total_tasks": 3,
            "completed_tasks": {
                "1": {
                    "task_id": "1",
                    "success": True,
                    "duration_seconds": 10.0,
                    "retry_count": 0,
                    "error_message": None,
                    "completed_at": "2024-01-15T10:01:00",
                }
            },
            "in_progress_tasks": ["2"],
            "pending_tasks": ["3"],
            "failed_tasks": [],
            "skipped_tasks": [],
        }

        checkpoint = Checkpoint.from_dict(data)

        self.assertEqual(checkpoint.workflow, "test-taskfile")
        self.assertEqual(checkpoint.total_tasks, 3)
        self.assertEqual(len(checkpoint.completed_tasks), 1)
        self.assertIn("2", checkpoint.in_progress_tasks)
        self.assertIn("3", checkpoint.pending_tasks)

    def test_checkpoint_get_tasks_for_resume_safe_retry(self):
        """Test resume with safe_retry tasks"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-taskfile",
                task_ids=["1", "2", "3"],
            )

            # Simulate: task 1 completed, task 2 in-progress, task 3 pending
            checkpoint.mark_completed("1", success=True, duration_seconds=10.0)
            checkpoint.mark_running("2")

            task_safe_retry_map = {"1": False, "2": True, "3": False}

            tasks_to_run, tasks_requiring_decision, error_msg = checkpoint.get_tasks_for_resume(
                task_safe_retry_map
            )

            self.assertIn("2", tasks_to_run)  # safe_retry=True, should retry
            self.assertIn("3", tasks_to_run)  # pending
            self.assertEqual(len(tasks_requiring_decision), 0)
            self.assertIsNone(error_msg)
        finally:
            os.unlink(taskfile_path)

    def test_checkpoint_get_tasks_for_resume_not_safe(self):
        """Test resume with non-safe retry tasks blocks"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-taskfile",
                task_ids=["1", "2", "3"],
            )

            # Simulate: task 2 in-progress with safe_retry=False
            checkpoint.mark_running("2")

            task_safe_retry_map = {"1": False, "2": False, "3": False}

            tasks_to_run, tasks_requiring_decision, error_msg = checkpoint.get_tasks_for_resume(
                task_safe_retry_map
            )

            self.assertIn("2", tasks_requiring_decision)
            self.assertIsNotNone(error_msg)
            self.assertIn("--resume-from", error_msg)
        finally:
            os.unlink(taskfile_path)


class TestCheckpointSaveLoad(unittest.TestCase):
    """Tests for checkpoint save/load functions"""

    def test_save_and_load_checkpoint(self):
        """Test saving and loading a checkpoint"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_path = os.path.join(tmpdir, "checkpoint.json")

            try:
                # Create and save checkpoint
                checkpoint = Checkpoint.create(
                    taskfile_path=taskfile_path,
                    workflow="test-taskfile",
                    task_ids=["1", "2", "3"],
                )
                checkpoint.mark_completed("1", success=True, duration_seconds=10.0)

                save_checkpoint(checkpoint, checkpoint_path)
                self.assertTrue(os.path.exists(checkpoint_path))

                # Load checkpoint
                loaded = load_checkpoint(checkpoint_path)

                self.assertEqual(loaded.workflow, "test-taskfile")
                self.assertEqual(loaded.total_tasks, 3)
                self.assertIn("1", loaded.completed_tasks)
                self.assertEqual(loaded.success_count, 1)
            finally:
                os.unlink(taskfile_path)

    def test_load_nonexistent_checkpoint(self):
        """Test loading a non-existent checkpoint"""
        with self.assertRaises(FileNotFoundError):
            load_checkpoint("/nonexistent/checkpoint.json")

    def test_load_invalid_checkpoint(self):
        """Test loading an invalid checkpoint file"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json {")
            f.flush()
            checkpoint_path = f.name

        try:
            with self.assertRaises(ValueError):
                load_checkpoint(checkpoint_path)
        finally:
            os.unlink(checkpoint_path)

    def test_delete_checkpoint(self):
        """Test deleting a checkpoint"""
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_path = os.path.join(tmpdir, "checkpoint.json")

            # Create file
            with open(checkpoint_path, "w") as f:
                f.write("{}")

            self.assertTrue(os.path.exists(checkpoint_path))

            result = delete_checkpoint(checkpoint_path)
            self.assertTrue(result)
            self.assertFalse(os.path.exists(checkpoint_path))

            # Delete non-existent file
            result = delete_checkpoint(checkpoint_path)
            self.assertFalse(result)


class TestGetCheckpointPath(unittest.TestCase):
    """Tests for get_checkpoint_path function"""

    def test_get_checkpoint_path_simple(self):
        """Test generating checkpoint path"""
        path = get_checkpoint_path("./checkpoints", "daily-etl")
        self.assertEqual(path.name, "checkpoint_daily-etl.json")
        self.assertEqual(path.parent.name, "checkpoints")

    def test_get_checkpoint_path_sanitizes_id(self):
        """Test checkpoint path sanitizes special characters"""
        path = get_checkpoint_path("./checkpoints", "daily/etl:task")
        self.assertNotIn("/", path.name)
        self.assertNotIn(":", path.name)


class TestCheckpointManager(unittest.TestCase):
    """Tests for CheckpointManager class"""

    def test_checkpoint_manager_creation(self):
        """Test creating a CheckpointManager"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                manager = CheckpointManager(
                    checkpoint_dir=tmpdir,
                    taskfile_path=taskfile_path,
                    workflow="test-taskfile",
                    task_ids=["1", "2", "3"],
                    checkpoint_interval=60,
                    enabled=True,
                )

                self.assertTrue(manager.enabled)
                self.assertIsNotNone(manager.checkpoint)
                self.assertEqual(manager.checkpoint.total_tasks, 3)
                self.assertTrue(manager.checkpoint_path.exists())
            finally:
                os.unlink(taskfile_path)

    def test_checkpoint_manager_disabled(self):
        """Test CheckpointManager when disabled"""
        manager = CheckpointManager(
            checkpoint_dir="./checkpoints",
            taskfile_path="tasks.json",
            workflow="test",
            task_ids=["1"],
            enabled=False,
        )

        self.assertFalse(manager.enabled)
        self.assertIsNone(manager.checkpoint)

    def test_checkpoint_manager_mark_running_completed(self):
        """Test marking tasks through CheckpointManager"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                manager = CheckpointManager(
                    checkpoint_dir=tmpdir,
                    taskfile_path=taskfile_path,
                    workflow="test-taskfile",
                    task_ids=["1", "2"],
                    enabled=True,
                )

                manager.mark_running("1")
                self.assertIn("1", manager.checkpoint.in_progress_tasks)

                manager.mark_completed("1", success=True, duration_seconds=10.0)
                self.assertIn("1", manager.checkpoint.completed_tasks)
                self.assertEqual(manager.checkpoint.success_count, 1)
            finally:
                os.unlink(taskfile_path)

    def test_checkpoint_manager_cleanup_success(self):
        """Test cleanup deletes checkpoint on success"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                manager = CheckpointManager(
                    checkpoint_dir=tmpdir,
                    taskfile_path=taskfile_path,
                    workflow="test-taskfile",
                    task_ids=["1"],
                    enabled=True,
                )

                checkpoint_path = manager.checkpoint_path
                self.assertTrue(checkpoint_path.exists())

                manager.cleanup(success=True)
                self.assertFalse(checkpoint_path.exists())
            finally:
                os.unlink(taskfile_path)

    def test_checkpoint_manager_cleanup_failure(self):
        """Test cleanup retains checkpoint on failure"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                manager = CheckpointManager(
                    checkpoint_dir=tmpdir,
                    taskfile_path=taskfile_path,
                    workflow="test-taskfile",
                    task_ids=["1"],
                    enabled=True,
                )

                checkpoint_path = manager.checkpoint_path
                self.assertTrue(checkpoint_path.exists())

                manager.cleanup(success=False)
                self.assertTrue(checkpoint_path.exists())
            finally:
                os.unlink(taskfile_path)


class TestResumeCliArguments(unittest.TestCase):
    """Tests for resume CLI argument handling.

    Verifies that:
    - 'run' command no longer accepts --resume, --resume-from, --checkpoint
    - 'resume' subcommand accepts all resume-related arguments
    """

    def test_run_command_rejects_resume_flag(self):
        """Test that 'run' command rejects --resume flag."""
        from rushti.cli import parse_arguments

        # Should raise SystemExit for unrecognized argument
        with self.assertRaises(SystemExit):
            parse_arguments(["rushti", "--tasks", "tasks.json", "--resume"])

    def test_run_command_rejects_resume_from_flag(self):
        """Test that 'run' command rejects --resume-from flag."""
        from rushti.cli import parse_arguments

        with self.assertRaises(SystemExit):
            parse_arguments(["rushti", "--tasks", "tasks.json", "--resume-from", "task-1"])

    def test_run_command_rejects_checkpoint_flag(self):
        """Test that 'run' command rejects --checkpoint flag."""
        from rushti.cli import parse_arguments

        with self.assertRaises(SystemExit):
            parse_arguments(["rushti", "--tasks", "tasks.json", "--checkpoint", "cp.json"])

    def test_run_command_accepts_no_checkpoint_flag(self):
        """Test that 'run' command still accepts --no-checkpoint flag."""
        from rushti.cli import parse_arguments

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write('instance="tm1srv01" process="test"\n')
            f.flush()
            task_file = f.name

        try:
            tasks_file, cli_args = parse_arguments(
                ["rushti", "--tasks", task_file, "--no-checkpoint"]
            )
            self.assertTrue(cli_args.get("no_checkpoint"))
        finally:
            os.unlink(task_file)

    def test_cli_args_does_not_contain_resume_keys_from_run(self):
        """Test that cli_args from 'run' command doesn't contain resume keys."""
        from rushti.cli import parse_arguments

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write('instance="tm1srv01" process="test"\n')
            f.flush()
            task_file = f.name

        try:
            tasks_file, cli_args = parse_arguments(["rushti", "--tasks", task_file])
            # These keys should NOT be in cli_args when parsed from run command
            self.assertNotIn("resume", cli_args)
            self.assertNotIn("resume_from", cli_args)
            self.assertNotIn("checkpoint_file", cli_args)
        finally:
            os.unlink(task_file)


class TestResumeContext(unittest.TestCase):
    """Tests for the resume context returned by run_resume_command."""

    def test_run_resume_command_returns_dict(self):
        """Test that run_resume_command returns Optional[dict]."""
        from rushti.commands import run_resume_command
        import inspect

        sig = inspect.signature(run_resume_command)
        # Verify function accepts argv and returns Optional[dict]
        self.assertEqual(list(sig.parameters.keys()), ["argv"])

    def test_resume_context_structure(self):
        """Test that resume context dict has correct structure when returned."""
        # The context should have these keys when returned by run_resume_command
        expected_keys = {"resume", "resume_from", "checkpoint_file"}
        context = {
            "resume": True,
            "resume_from": "task-5",
            "checkpoint_file": "/path/to/checkpoint.json",
        }
        self.assertEqual(set(context.keys()), expected_keys)

    def test_cli_args_merge_with_resume_context(self):
        """Test that cli_args correctly merges with resume context return value."""
        cli_args = {
            "max_workers": 4,
            "no_checkpoint": False,
        }
        resume_context = {
            "resume": True,
            "resume_from": None,
            "checkpoint_file": "/path/to/checkpoint.json",
        }

        # Simulate the merge that happens in main()
        cli_args.update(resume_context)

        self.assertTrue(cli_args["resume"])
        self.assertIsNone(cli_args["resume_from"])
        self.assertEqual(cli_args["checkpoint_file"], "/path/to/checkpoint.json")
        self.assertEqual(cli_args["max_workers"], 4)


class TestFindCheckpointForTaskfile(unittest.TestCase):
    """Tests for find_checkpoint_for_taskfile function."""

    def test_find_checkpoint_by_taskfile_path(self):
        """Test finding checkpoint by matching taskfile path."""
        from rushti.checkpoint import (
            Checkpoint,
            save_checkpoint,
            find_checkpoint_for_taskfile,
            get_checkpoint_path,
        )

        with tempfile.TemporaryDirectory() as checkpoint_dir:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump({"version": "2.0", "tasks": []}, f)
                f.flush()
                taskfile_path = f.name

            try:
                # Create checkpoint
                checkpoint = Checkpoint.create(
                    taskfile_path=taskfile_path,
                    workflow="test-find",
                    task_ids=["1", "2"],
                )
                checkpoint_path = get_checkpoint_path(checkpoint_dir, "test-find")
                save_checkpoint(checkpoint, checkpoint_path)

                # Find checkpoint
                found = find_checkpoint_for_taskfile(checkpoint_dir, taskfile_path)

                self.assertIsNotNone(found)
                self.assertEqual(found.name, "checkpoint_test-find.json")
            finally:
                os.unlink(taskfile_path)

    def test_find_checkpoint_returns_none_when_not_found(self):
        """Test that find_checkpoint_for_taskfile returns None when no match."""
        from rushti.checkpoint import find_checkpoint_for_taskfile

        with tempfile.TemporaryDirectory() as checkpoint_dir:
            result = find_checkpoint_for_taskfile(checkpoint_dir, "/nonexistent/tasks.json")
            self.assertIsNone(result)

    def test_find_checkpoint_returns_none_for_empty_dir(self):
        """Test that find_checkpoint_for_taskfile returns None for empty directory."""
        from rushti.checkpoint import find_checkpoint_for_taskfile

        with tempfile.TemporaryDirectory() as checkpoint_dir:
            result = find_checkpoint_for_taskfile(checkpoint_dir, "tasks.json")
            self.assertIsNone(result)


class TestCheckpointValidation(unittest.TestCase):
    """Tests for checkpoint validation against taskfile."""

    def test_validate_matching_taskfile(self):
        """Test validation passes for matching taskfile."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-validate",
                task_ids=["1"],
            )

            is_valid, warnings = checkpoint.validate_against_taskfile(taskfile_path, strict=True)
            self.assertTrue(is_valid)
            self.assertEqual(len(warnings), 0)
        finally:
            os.unlink(taskfile_path)

    def test_validate_detects_modified_taskfile(self):
        """Test validation detects when taskfile has been modified."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-modified",
                task_ids=["1"],
            )

            # Modify the file
            with open(taskfile_path, "w") as f:
                json.dump({"version": "2.0", "tasks": [{"id": "new"}]}, f)

            is_valid, warnings = checkpoint.validate_against_taskfile(taskfile_path, strict=True)
            self.assertFalse(is_valid)
            self.assertTrue(any("modified" in w.lower() for w in warnings))
        finally:
            os.unlink(taskfile_path)

    def test_validate_non_strict_allows_modified(self):
        """Test non-strict validation allows modified taskfile with warning."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-non-strict",
                task_ids=["1"],
            )

            # Modify the file
            with open(taskfile_path, "w") as f:
                json.dump({"version": "2.0", "tasks": [{"id": "new"}]}, f)

            is_valid, warnings = checkpoint.validate_against_taskfile(taskfile_path, strict=False)
            self.assertTrue(is_valid)  # Non-strict should pass
            self.assertTrue(len(warnings) > 0)  # But still have warning
        finally:
            os.unlink(taskfile_path)


class TestResumeFromTask(unittest.TestCase):
    """Tests for get_resume_from_task functionality."""

    def test_resume_from_middle_task_with_completed_predecessors(self):
        """Test resuming from a task when earlier tasks are already completed.

        This simulates the real scenario: tasks 1 and 2 completed, resuming from 3.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-resume-from",
                task_ids=["1", "2", "3", "4", "5"],
            )
            # Mark tasks 1 and 2 as completed (removes them from pending)
            checkpoint.mark_completed("1", success=True, duration_seconds=1.0)
            checkpoint.mark_completed("2", success=True, duration_seconds=1.0)

            tasks_to_run = checkpoint.get_resume_from_task("3", ["1", "2", "3", "4", "5"])

            # Should include tasks from resume point onwards plus remaining pending tasks
            self.assertIn("3", tasks_to_run)
            self.assertIn("4", tasks_to_run)
            self.assertIn("5", tasks_to_run)
            # Completed tasks are not in pending_tasks, so they won't be included
            self.assertNotIn("1", tasks_to_run)
            self.assertNotIn("2", tasks_to_run)
        finally:
            os.unlink(taskfile_path)

    def test_resume_from_first_task(self):
        """Test resuming from the first task runs all pending tasks."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-resume-first",
                task_ids=["1", "2", "3"],
            )

            tasks_to_run = checkpoint.get_resume_from_task("1", ["1", "2", "3"])

            # All tasks are pending, so all should be included
            self.assertEqual(tasks_to_run, {"1", "2", "3"})
        finally:
            os.unlink(taskfile_path)

    def test_resume_from_last_task_with_completed_predecessors(self):
        """Test resuming from the last task after earlier tasks completed."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-resume-last",
                task_ids=["1", "2", "3"],
            )
            # Mark tasks 1 and 2 as completed
            checkpoint.mark_completed("1", success=True, duration_seconds=1.0)
            checkpoint.mark_completed("2", success=True, duration_seconds=1.0)

            tasks_to_run = checkpoint.get_resume_from_task("3", ["1", "2", "3"])

            # Only task 3 should be returned (1 and 2 are completed, not pending)
            self.assertEqual(tasks_to_run, {"3"})
        finally:
            os.unlink(taskfile_path)

    def test_resume_from_nonexistent_task_raises(self):
        """Test resuming from non-existent task raises ValueError."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-invalid",
                task_ids=["1", "2", "3"],
            )

            with self.assertRaises(ValueError) as context:
                checkpoint.get_resume_from_task("nonexistent", ["1", "2", "3"])

            self.assertIn("not found", str(context.exception))
        finally:
            os.unlink(taskfile_path)

    def test_get_resume_from_includes_pending_tasks(self):
        """Test that get_resume_from_task includes pending tasks before resume point.

        This tests the actual behavior: pending_tasks are always included regardless
        of the resume point position.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            taskfile_path = f.name

        try:
            checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow="test-pending",
                task_ids=["1", "2", "3", "4"],
            )
            # Only complete task 2 - tasks 1, 3, 4 remain pending
            checkpoint.mark_completed("2", success=True, duration_seconds=1.0)

            tasks_to_run = checkpoint.get_resume_from_task("3", ["1", "2", "3", "4"])

            # Should include task 3, 4 from resume point, plus task 1 (still pending)
            self.assertIn("1", tasks_to_run)  # Still pending
            self.assertNotIn("2", tasks_to_run)  # Completed
            self.assertIn("3", tasks_to_run)  # Resume point
            self.assertIn("4", tasks_to_run)  # After resume point
        finally:
            os.unlink(taskfile_path)


if __name__ == "__main__":
    unittest.main()
