"""Integration tests for the tasks command with TM1.

These tests require:
- TM1 instance configured in tests/config.ini or RUSHTI_TEST_CONFIG environment variable
- rushti cube with task definitions (from rushti build command)

Run with: pytest tests/integration/test_tasks_command.py -v -m requires_tm1
"""

import json
import os
import sys
import tempfile
import unittest

import pytest

# Path setup handled by conftest.py, but also support direct execution
_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from conftest import get_test_tm1_config, get_test_tm1_names  # noqa: E402

_integration_path = os.path.dirname(os.path.abspath(__file__))
if _integration_path not in sys.path:
    sys.path.insert(0, _integration_path)


@pytest.mark.requires_tm1
class TestTasksExportFromTM1(unittest.TestCase):
    """Integration tests for tasks --export from TM1."""

    @classmethod
    def setUpClass(cls):
        """Set up TM1 connection once for all tests."""
        tm1_config, config_source = get_test_tm1_config()
        if tm1_config is None:
            cls.tm1_available = False
            cls.tm1_instance = "tm1srv01"
            cls.config_path = None
            return

        cls.tm1_available = True
        cls.tm1_instance = tm1_config.instance
        cls.config_path = config_source
        cls.tm1_config = tm1_config

        # Ensure test TM1 objects exist (cube, dimensions, sample data)
        from TM1py import TM1Service
        from tm1_setup import setup_tm1_test_objects
        from rushti.tm1_build import _populate_sample_data

        tm1_names = get_test_tm1_names()
        try:
            with TM1Service(**tm1_config.to_dict()) as tm1:
                setup_tm1_test_objects(tm1, **tm1_names)
                _populate_sample_data(tm1, tm1_names["cube_name"])
        except Exception as e:
            print(f"Warning: Failed to setup test objects: {e}")

    def setUp(self):
        """Skip test if TM1 not available."""
        if not self.tm1_available:
            self.skipTest("TM1 configuration not available")

    def test_export_from_tm1_to_json(self):
        """Test exporting a taskfile from TM1 cube to JSON file."""
        from rushti.taskfile import load_taskfile_from_source, TaskfileSource

        # Create a TaskfileSource for TM1
        # Note: This requires a workflow that exists in the rushti cube
        # Using a test workflow that should exist from the build command
        source = TaskfileSource(
            tm1_instance=self.tm1_instance,
            workflow="Sample_Optimal_Mode",  # Standard test taskfile
        )

        tm1_names = get_test_tm1_names()
        try:
            # Load from TM1
            taskfile = load_taskfile_from_source(source, self.config_path, mode="opt", **tm1_names)

            # Verify taskfile was loaded
            self.assertIsNotNone(taskfile)
            self.assertGreater(len(taskfile.tasks), 0)

            # Export to JSON
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(taskfile.to_dict(), f, indent=2)
                output_path = f.name

            try:
                # Verify JSON file is valid
                self.assertTrue(os.path.exists(output_path))

                with open(output_path, "r") as f:
                    exported_data = json.load(f)

                self.assertIn("version", exported_data)
                self.assertIn("tasks", exported_data)
                self.assertGreater(len(exported_data["tasks"]), 0)

            finally:
                if os.path.exists(output_path):
                    os.unlink(output_path)

        except Exception as e:
            # If the taskfile doesn't exist in TM1, skip gracefully
            if "not found" in str(e).lower() or "no tasks" in str(e).lower():
                self.skipTest(f"Test taskfile not available in TM1: {e}")
            raise

    def test_export_from_tm1_norm_mode(self):
        """Test exporting from TM1 with norm mode (wait-based sequencing)."""
        from rushti.taskfile import load_taskfile_from_source, TaskfileSource

        source = TaskfileSource(
            tm1_instance=self.tm1_instance,
            workflow="Sample_Stage_Mode",  # Taskfile with wait markers
        )

        tm1_names = get_test_tm1_names()
        try:
            # Load with norm mode
            taskfile = load_taskfile_from_source(source, self.config_path, mode="norm", **tm1_names)

            self.assertIsNotNone(taskfile)

            # In norm mode, tasks after wait markers should have predecessors
            # Verify the taskfile loaded successfully and has tasks
            self.assertGreater(len(taskfile.tasks), 0)

            # Note: In norm mode, wait markers cause subsequent tasks to have predecessors
            # We don't assert this as it depends on the specific taskfile structure

        except Exception as e:
            if "not found" in str(e).lower() or "no tasks" in str(e).lower():
                self.skipTest(f"Test taskfile not available in TM1: {e}")
            raise


@pytest.mark.requires_tm1
class TestTasksPushToTM1(unittest.TestCase):
    """Integration tests for tasks --push to TM1."""

    @classmethod
    def setUpClass(cls):
        """Set up TM1 connection once for all tests."""
        tm1_config, config_source = get_test_tm1_config()
        if tm1_config is None:
            cls.tm1_available = False
            cls.tm1_instance = "tm1srv01"
            cls.config_path = None
            return

        cls.tm1_available = True
        cls.tm1_instance = tm1_config.instance
        cls.config_path = config_source
        cls.tm1_config = tm1_config

    def setUp(self):
        """Skip test if TM1 not available."""
        if not self.tm1_available:
            self.skipTest("TM1 configuration not available")

    def test_push_json_taskfile_to_tm1(self):
        """Test pushing a JSON taskfile to TM1 as a file."""
        from TM1py import TM1Service
        from rushti.taskfile import parse_json_taskfile

        # Create a test JSON taskfile
        test_data = {
            "version": "2.0",
            "metadata": {"workflow": "test-push-integration", "name": "Integration Test Push"},
            "tasks": [
                {
                    "id": "1",
                    "instance": self.tm1_instance,
                    "process": "}bedrock.server.wait",
                    "parameters": {"pWaitSec": "1"},
                }
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_data, f, indent=2)
            taskfile_path = f.name

        try:
            # Parse and validate the taskfile
            taskfile = parse_json_taskfile(taskfile_path)
            workflow = taskfile.metadata.workflow

            # Connect to TM1 and push
            with TM1Service(**self.tm1_config.to_dict()) as tm1:
                # Read file content
                with open(taskfile_path, "rb") as f:
                    file_content = f.read()

                file_name = f"rushti_taskfile_{workflow}.json"

                # Delete if exists (cleanup from previous test runs)
                try:
                    tm1.files.delete(file_name)
                except Exception:
                    pass  # File didn't exist

                # Upload to TM1
                tm1.files.create(file_name=file_name, file_content=file_content)

                # Verify file exists
                files = tm1.files.get_all_names()
                self.assertIn(file_name, files)

                # Cleanup: delete the test file
                tm1.files.delete(file_name)

        finally:
            if os.path.exists(taskfile_path):
                os.unlink(taskfile_path)

    def test_push_and_retrieve_roundtrip(self):
        """Test that pushed JSON can be retrieved and parsed correctly."""
        from TM1py import TM1Service

        # Create test taskfile with various properties
        test_data = {
            "version": "2.0",
            "metadata": {"workflow": "test-roundtrip-push", "name": "Roundtrip Push Test"},
            "settings": {"max_workers": 4, "retries": 2},
            "tasks": [
                {
                    "id": "extract-1",
                    "instance": self.tm1_instance,
                    "process": "}bedrock.server.wait",
                    "parameters": {"pWaitSec": "1"},
                    "stage": "Extract",
                    "safe_retry": True,
                },
                {
                    "id": "transform-1",
                    "instance": self.tm1_instance,
                    "process": "}bedrock.server.wait",
                    "parameters": {"pWaitSec": "2"},
                    "predecessors": ["extract-1"],
                    "stage": "Transform",
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_data, f, indent=2)
            taskfile_path = f.name

        try:
            workflow = test_data["metadata"]["workflow"]
            file_name = f"rushti_taskfile_{workflow}.json"

            with TM1Service(**self.tm1_config.to_dict()) as tm1:
                # Cleanup first
                try:
                    tm1.files.delete(file_name)
                except Exception:
                    pass

                # Upload
                with open(taskfile_path, "rb") as f:
                    tm1.files.create(file_name=file_name, file_content=f.read())

                # Retrieve
                retrieved_content = tm1.files.get(file_name)
                retrieved_data = json.loads(retrieved_content.decode("utf-8"))

                # Verify all properties preserved
                self.assertEqual(retrieved_data["version"], "2.0")
                self.assertEqual(retrieved_data["metadata"]["workflow"], "test-roundtrip-push")
                self.assertEqual(retrieved_data["settings"]["max_workers"], 4)
                self.assertEqual(len(retrieved_data["tasks"]), 2)

                # Verify task properties
                task1 = retrieved_data["tasks"][0]
                self.assertEqual(task1["id"], "extract-1")
                self.assertEqual(task1["stage"], "Extract")
                self.assertTrue(task1["safe_retry"])

                task2 = retrieved_data["tasks"][1]
                self.assertEqual(task2["predecessors"], ["extract-1"])

                # Cleanup
                tm1.files.delete(file_name)

        finally:
            if os.path.exists(taskfile_path):
                os.unlink(taskfile_path)


if __name__ == "__main__":
    unittest.main()
