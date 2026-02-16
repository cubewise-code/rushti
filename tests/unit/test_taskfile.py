"""
Unit tests for taskfile functionality.
Covers JSON task file parsing, validation, TXT conversion, and task definitions.
"""

import json
import os
import tempfile
import unittest
import unittest.mock

from rushti.taskfile import (
    parse_json_taskfile,
    detect_file_type,
    convert_txt_to_json,
    validate_taskfile,
    archive_taskfile,
    TaskDefinition,
    Taskfile,
    TaskfileMetadata,
    TaskfileValidationError,
    TaskfileSource,
)
from rushti.taskfile import parse_line_arguments
from rushti.task import Task, OptimizedTask


class TestJSONTaskfileValidation(unittest.TestCase):
    """Tests for JSON task file validation"""

    def test_validate_valid_taskfile(self):
        """Test validation of a valid task file"""
        data = {
            "version": "2.0",
            "metadata": {"workflow": "test-001"},
            "tasks": [{"id": "1", "instance": "tm1srv01", "process": "test.process"}],
        }
        errors = validate_taskfile(data)
        self.assertEqual(errors, [])

    def test_validate_missing_version(self):
        """Test validation catches missing version"""
        data = {"tasks": [{"id": "1", "instance": "tm1srv01", "process": "test.process"}]}
        errors = validate_taskfile(data)
        self.assertIn("Missing 'version' field", errors)

    def test_validate_missing_tasks(self):
        """Test validation catches missing tasks"""
        data = {"version": "2.0"}
        errors = validate_taskfile(data)
        self.assertIn("Missing 'tasks' array", errors)

    def test_validate_empty_tasks(self):
        """Test validation catches empty tasks array"""
        data = {"version": "2.0", "tasks": []}
        errors = validate_taskfile(data)
        self.assertIn("'tasks' array cannot be empty", errors)

    def test_validate_missing_required_properties(self):
        """Test validation catches missing required task properties"""
        data = {"version": "2.0", "tasks": [{"id": "1"}]}  # Missing instance and process
        errors = validate_taskfile(data)
        self.assertTrue(any("Missing required property 'instance'" in e for e in errors))
        self.assertTrue(any("Missing required property 'process'" in e for e in errors))

    def test_validate_duplicate_task_ids(self):
        """Test validation catches duplicate task IDs"""
        data = {
            "version": "2.0",
            "tasks": [
                {"id": "1", "instance": "tm1srv01", "process": "test.process"},
                {"id": "1", "instance": "tm1srv01", "process": "test.process2"},
            ],
        }
        errors = validate_taskfile(data)
        self.assertTrue(any("Duplicate task ID '1'" in e for e in errors))

    def test_validate_invalid_max_workers(self):
        """Test validation catches invalid max_workers"""
        data = {
            "version": "2.0",
            "settings": {"max_workers": 0},
            "tasks": [{"id": "1", "instance": "tm1srv01", "process": "test.process"}],
        }
        errors = validate_taskfile(data)
        self.assertTrue(any("max_workers must be a positive integer" in e for e in errors))


class TestJSONTaskfileParsing(unittest.TestCase):
    """Tests for JSON task file parsing"""

    def test_parse_json_taskfile(self):
        """Test parsing a valid JSON task file"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_content = {
                "version": "2.0",
                "metadata": {"workflow": "test-001", "name": "Test Task File"},
                "settings": {"max_workers": 8, "retries": 2},
                "tasks": [
                    {
                        "id": "1",
                        "instance": "tm1srv01",
                        "process": "test.process",
                        "parameters": {"pParam1": "value1"},
                        "predecessors": [],
                        "stage": "Extract",
                    },
                    {
                        "id": "2",
                        "instance": "tm1srv01",
                        "process": "test.process2",
                        "predecessors": ["1"],
                        "stage": "Transform",
                    },
                ],
            }
            json.dump(json_content, f)
            f.flush()
            file_path = f.name

        try:
            taskfile = parse_json_taskfile(file_path)
            self.assertEqual(taskfile.version, "2.0")
            self.assertEqual(taskfile.metadata.workflow, "test-001")
            self.assertEqual(taskfile.settings.max_workers, 8)
            self.assertEqual(len(taskfile.tasks), 2)
            self.assertEqual(taskfile.tasks[0].id, "1")
            self.assertEqual(taskfile.tasks[0].stage, "Extract")
            self.assertEqual(taskfile.tasks[1].predecessors, ["1"])
        finally:
            os.unlink(file_path)

    def test_parse_invalid_json(self):
        """Test parsing invalid JSON raises error"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ invalid json }")
            f.flush()
            file_path = f.name

        try:
            with self.assertRaises(TaskfileValidationError):
                parse_json_taskfile(file_path)
        finally:
            os.unlink(file_path)

    def test_parse_validation_error(self):
        """Test parsing file with validation errors raises error"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"version": "2.0", "tasks": []}, f)
            f.flush()
            file_path = f.name

        try:
            with self.assertRaises(TaskfileValidationError):
                parse_json_taskfile(file_path)
        finally:
            os.unlink(file_path)


class TestFileTypeDetection(unittest.TestCase):
    """Tests for file type detection"""

    def test_detect_json_extension(self):
        """Test detection of .json extension"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{}")
            f.flush()
            file_path = f.name

        try:
            self.assertEqual(detect_file_type(file_path), "json")
        finally:
            os.unlink(file_path)

    def test_detect_txt_extension(self):
        """Test detection of .txt extension"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("test")
            f.flush()
            file_path = f.name

        try:
            self.assertEqual(detect_file_type(file_path), "txt")
        finally:
            os.unlink(file_path)

    def test_detect_json_content(self):
        """Test detection of JSON content without .json extension"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tasks", delete=False) as f:
            f.write('{"version": "2.0"}')
            f.flush()
            file_path = f.name

        try:
            self.assertEqual(detect_file_type(file_path), "json")
        finally:
            os.unlink(file_path)


class TestTXTToJSONConversion(unittest.TestCase):
    """Tests for TXT to JSON conversion"""

    def test_convert_simple_txt(self):
        """Test converting a simple TXT file"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write('instance="tm1srv01" process="test.process" pParam1="value1"\n')
            f.write('instance="tm1srv01" process="test.process2" pParam2="value2"\n')
            f.flush()
            file_path = f.name

        try:
            taskfile = convert_txt_to_json(file_path)
            self.assertEqual(len(taskfile.tasks), 2)
            self.assertEqual(taskfile.tasks[0].instance, "tm1srv01")
            self.assertEqual(taskfile.tasks[0].process, "test.process")
            self.assertEqual(taskfile.tasks[0].parameters.get("pParam1"), "value1")
        finally:
            os.unlink(file_path)

    def test_convert_txt_with_wait(self):
        """Test converting TXT file with wait keyword adds predecessors"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write('instance="tm1srv01" process="test.process1"\n')
            f.write("wait\n")
            f.write('instance="tm1srv01" process="test.process2"\n')
            f.flush()
            file_path = f.name

        try:
            taskfile = convert_txt_to_json(file_path)
            self.assertEqual(len(taskfile.tasks), 2)
            # Second task should have first task as predecessor
            self.assertIn(taskfile.tasks[0].id, taskfile.tasks[1].predecessors)
        finally:
            os.unlink(file_path)

    def test_convert_txt_save_output(self):
        """Test converting TXT and saving to JSON file"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as txt_f:
            txt_f.write('instance="tm1srv01" process="test.process"\n')
            txt_f.flush()
            txt_path = txt_f.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as json_f:
            json_path = json_f.name

        try:
            convert_txt_to_json(txt_path, json_path)
            # Verify file was created
            self.assertTrue(os.path.exists(json_path))
            # Verify content is valid JSON
            parsed = parse_json_taskfile(json_path)
            self.assertEqual(len(parsed.tasks), 1)
        finally:
            os.unlink(txt_path)
            if os.path.exists(json_path):
                os.unlink(json_path)


class TestTaskDefinition(unittest.TestCase):
    """Tests for TaskDefinition class"""

    def test_task_definition_defaults(self):
        """Test TaskDefinition default values"""
        task = TaskDefinition(id="1", instance="tm1", process="test")
        self.assertEqual(task.id, "1")
        self.assertEqual(task.parameters, {})
        self.assertEqual(task.predecessors, [])
        self.assertIsNone(task.stage)
        self.assertFalse(task.safe_retry)
        self.assertIsNone(task.timeout)
        self.assertFalse(task.cancel_at_timeout)
        self.assertFalse(task.require_predecessor_success)
        self.assertFalse(task.succeed_on_minor_errors)

    def test_task_definition_to_dict(self):
        """Test TaskDefinition.to_dict() only includes non-default values"""
        task = TaskDefinition(id="1", instance="tm1", process="test", stage="Extract", timeout=300)
        d = task.to_dict()
        self.assertEqual(d["id"], "1")
        self.assertEqual(d["stage"], "Extract")
        self.assertEqual(d["timeout"], 300)
        # Default values should not be included
        self.assertNotIn("safe_retry", d)
        self.assertNotIn("predecessors", d)

    def test_task_definition_from_dict(self):
        """Test TaskDefinition.from_dict()"""
        data = {
            "id": "1",
            "instance": "tm1",
            "process": "test",
            "predecessors": ["0"],
            "stage": "Extract",
            "timeout": 300,
        }
        task = TaskDefinition.from_dict(data)
        self.assertEqual(task.id, "1")
        self.assertEqual(task.predecessors, ["0"])
        self.assertEqual(task.stage, "Extract")
        self.assertEqual(task.timeout, 300)


class TestParseLineArguments(unittest.TestCase):
    """Tests for parsing task line arguments"""

    def test_basic_arguments(self):
        line = 'instance=tm1 process=process1 param1="value1" param2="value 2"'
        result = parse_line_arguments(line)
        expected = {
            "instance": "tm1",
            "process": "process1",
            "param1": "value1",
            "param2": "value 2",
        }
        self.assertEqual(result, expected)

    def test_nested_double_quotes(self):
        line = 'instance=tm1 process=process1 param1="value with \\"quotes\\"" param2="simple"'
        result = parse_line_arguments(line)
        expected = {
            "instance": "tm1",
            "process": "process1",
            "param1": 'value with "quotes"',
            "param2": "simple",
        }
        self.assertEqual(result, expected)

    def test_backslashes(self):
        line = r'instance=tm1 process=process1 param1="value\\with\\backslashes" param2="normal"'
        result = parse_line_arguments(line)
        expected = {
            "instance": "tm1",
            "process": "process1",
            "param1": r"value\with\backslashes",
            "param2": "normal",
        }
        self.assertEqual(result, expected)

    def test_complex_nested_quotes(self):
        line = r'instance=tm1 process=process1 param1="outer \"inner \\\"deepest\\\" inner\" outer"'
        result = parse_line_arguments(line)
        expected = {
            "instance": "tm1",
            "process": "process1",
            "param1": 'outer "inner \\"deepest\\" inner" outer',
        }
        self.assertEqual(result, expected)

    def test_predecessors_and_require_predecessor_success(self):
        line = 'id=1 instance=tm1 process=process1 predecessors="2,3,4" require_predecessor_success="true"'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "instance": "tm1",
            "process": "process1",
            "predecessors": ["2", "3", "4"],
            "require_predecessor_success": True,
        }
        self.assertEqual(result, expected)

    def test_sql_query_parsing(self):
        self.maxDiff = None
        line = 'id="1" predecessors="" require_predecessor_success="" instance="tm1srv01" process="}bedrock.server.query" pQuery="SELECT Id,IsDeleted FROM Account WHERE date=\\"20241031092120\\"" pParam2="" pParam3="testing\\"2\\""'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "predecessors": [],
            "require_predecessor_success": False,
            "instance": "tm1srv01",
            "process": "}bedrock.server.query",
            "pQuery": 'SELECT Id,IsDeleted FROM Account WHERE date="20241031092120"',
            "pParam2": "",
            "pParam3": 'testing"2"',
        }
        self.assertEqual(result, expected)


class TestSucceedOnMinorErrors(unittest.TestCase):
    """Tests for succeed_on_minor_errors parameter"""

    def test_default_value(self):
        line = 'id=1 instance=tm1 process=process1 predecessors="2,3,4" require_predecessor_success="1"'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "instance": "tm1",
            "process": "process1",
            "predecessors": ["2", "3", "4"],
            "require_predecessor_success": True,
        }
        self.assertEqual(result, expected)

    def test_explicit_false_value(self):
        line = 'id=1 instance=tm1 process=process1 predecessors="2,3,4" require_predecessor_success="1" succeed_on_minor_errors="0"'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "instance": "tm1",
            "process": "process1",
            "predecessors": ["2", "3", "4"],
            "require_predecessor_success": True,
            "succeed_on_minor_errors": False,
        }
        self.assertEqual(result, expected)

    def test_explicit_true_value(self):
        line = 'id=1 instance=tm1 process=process1 predecessors="" require_predecessor_success="" succeed_on_minor_errors="1"'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "instance": "tm1",
            "process": "process1",
            "predecessors": [],
            "require_predecessor_success": False,
            "succeed_on_minor_errors": True,
        }
        self.assertEqual(result, expected)

    def test_line_translation_with_succeed_on_minor_errors(self):
        task = OptimizedTask(
            "1",
            "tm1srv01",
            "process1",
            {"param1": "value1"},
            [],
            False,
            succeed_on_minor_errors=True,
        )
        expected_line = 'id="1" predecessors="" require_predecessor_success="False" succeed_on_minor_errors="True" instance="tm1srv01" process="process1" param1="value1"\n'
        self.assertEqual(task.translate_to_line(), expected_line)

    def test_line_translation_without_succeed_on_minor_errors(self):
        task = OptimizedTask("1", "tm1srv01", "process1", {"param1": "value1"}, [2, 3, 4], False)
        expected_line = 'id="1" predecessors="2,3,4" require_predecessor_success="False" succeed_on_minor_errors="False" instance="tm1srv01" process="process1" param1="value1"\n'
        self.assertEqual(task.translate_to_line(), expected_line)


class TestNewTaskParameters(unittest.TestCase):
    """Tests for new task parameters (safe_retry, stage, timeout, cancel_at_timeout)"""

    def test_task_default_values(self):
        """Test that new parameters have correct defaults"""
        task = Task("tm1srv01", "process1", {})
        self.assertFalse(task.safe_retry)
        self.assertIsNone(task.stage)
        self.assertIsNone(task.timeout)
        self.assertFalse(task.cancel_at_timeout)

    def test_task_with_all_parameters(self):
        """Test Task with all new parameters set"""
        task = Task(
            "tm1srv01",
            "process1",
            {"param1": "value1"},
            succeed_on_minor_errors=True,
            safe_retry=True,
            stage="Extract",
            timeout=300,
            cancel_at_timeout=True,
        )
        self.assertTrue(task.safe_retry)
        self.assertEqual(task.stage, "Extract")
        self.assertEqual(task.timeout, 300)
        self.assertTrue(task.cancel_at_timeout)

    def test_optimized_task_with_all_parameters(self):
        """Test OptimizedTask with all new parameters"""
        task = OptimizedTask(
            "1",
            "tm1srv01",
            "process1",
            {"param1": "value1"},
            ["2", "3"],
            True,
            succeed_on_minor_errors=False,
            safe_retry=True,
            stage="Transform",
            timeout=600,
            cancel_at_timeout=True,
        )
        self.assertTrue(task.safe_retry)
        self.assertEqual(task.stage, "Transform")
        self.assertEqual(task.timeout, 600)
        self.assertTrue(task.cancel_at_timeout)

    def test_parse_new_parameters(self):
        """Test parsing new parameters from line arguments"""
        line = 'id="1" instance="tm1srv01" process="process1" safe_retry="true" stage="Load" timeout="120" cancel_at_timeout="1"'
        result = parse_line_arguments(line)
        self.assertTrue(result["safe_retry"])
        self.assertEqual(result["stage"], "Load")
        self.assertEqual(result["timeout"], 120)
        self.assertTrue(result["cancel_at_timeout"])

    def test_parse_empty_stage(self):
        """Test parsing empty stage value"""
        line = 'id="1" instance="tm1srv01" process="process1" stage=""'
        result = parse_line_arguments(line)
        self.assertIsNone(result.get("stage"))

    def test_parse_empty_timeout(self):
        """Test parsing empty timeout value"""
        line = 'id="1" instance="tm1srv01" process="process1" timeout=""'
        result = parse_line_arguments(line)
        self.assertIsNone(result.get("timeout"))

    def test_task_translate_to_line_with_new_params(self):
        """Test Task.translate_to_line includes new parameters when set"""
        task = Task(
            "tm1srv01",
            "process1",
            {"param1": "value1"},
            safe_retry=True,
            stage="Extract",
            timeout=300,
            cancel_at_timeout=True,
        )
        line = task.translate_to_line()
        self.assertIn('safe_retry="True"', line)
        self.assertIn('stage="Extract"', line)
        self.assertIn('timeout="300"', line)
        self.assertIn('cancel_at_timeout="True"', line)

    def test_optimized_task_translate_to_line_with_new_params(self):
        """Test OptimizedTask.translate_to_line includes new parameters"""
        task = OptimizedTask(
            "1",
            "tm1srv01",
            "process1",
            {},
            [],
            False,
            safe_retry=True,
            stage="Load",
            timeout=60,
            cancel_at_timeout=True,
        )
        line = task.translate_to_line()
        self.assertIn('safe_retry="True"', line)
        self.assertIn('stage="Load"', line)
        self.assertIn('timeout="60"', line)
        self.assertIn('cancel_at_timeout="True"', line)


class TestTaskfileSource(unittest.TestCase):
    """Tests for TaskfileSource dataclass"""

    def test_file_source(self):
        """Test TaskfileSource with file path"""
        source = TaskfileSource(file_path="tasks.json")
        self.assertTrue(source.is_file_source())
        self.assertFalse(source.is_tm1_source())
        # Should not raise
        source.validate()

    def test_tm1_source(self):
        """Test TaskfileSource with TM1 instance"""
        source = TaskfileSource(tm1_instance="tm1srv01", workflow="DailyETL")
        self.assertFalse(source.is_file_source())
        self.assertTrue(source.is_tm1_source())
        # Should not raise
        source.validate()

    def test_empty_source_invalid(self):
        """Test that empty TaskfileSource is invalid"""
        source = TaskfileSource()
        with self.assertRaises(ValueError) as context:
            source.validate()
        self.assertIn("either", str(context.exception).lower())

    def test_both_sources_invalid(self):
        """Test that specifying both file and TM1 is invalid"""
        source = TaskfileSource(
            file_path="tasks.json", tm1_instance="tm1srv01", workflow="DailyETL"
        )
        with self.assertRaises(ValueError) as context:
            source.validate()
        self.assertIn("cannot specify both", str(context.exception).lower())

    def test_tm1_without_workflow_invalid(self):
        """Test that TM1 source without workflow is invalid"""
        source = TaskfileSource(tm1_instance="tm1srv01")
        with self.assertRaises(ValueError) as context:
            source.validate()
        self.assertIn("workflow", str(context.exception).lower())

    def test_workflow_without_tm1_invalid(self):
        """Test that workflow without tm1_instance is invalid"""
        source = TaskfileSource(workflow="DailyETL")
        with self.assertRaises(ValueError) as context:
            source.validate()
        self.assertIn("tm1-instance", str(context.exception).lower())

    def test_from_args_file_source(self):
        """Test TaskfileSource.from_args with file source"""

        # Simulate argparse namespace
        class Args:
            taskfile = "tasks.json"
            tm1_instance = None
            workflow = None

        source = TaskfileSource.from_args(Args())
        self.assertTrue(source.is_file_source())
        self.assertEqual(source.file_path, "tasks.json")

    def test_from_args_tm1_source(self):
        """Test TaskfileSource.from_args with TM1 source"""

        class Args:
            taskfile = None
            tm1_instance = "tm1srv01"
            workflow = "DailyETL"

        source = TaskfileSource.from_args(Args())
        self.assertTrue(source.is_tm1_source())
        self.assertEqual(source.tm1_instance, "tm1srv01")
        self.assertEqual(source.workflow, "DailyETL")


class TestTasksExportCommand(unittest.TestCase):
    """Tests for tasks --export command functionality"""

    def test_export_txt_to_json(self):
        """Test exporting TXT taskfile to JSON"""
        # Create a TXT taskfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as txt_f:
            txt_f.write('instance="tm1srv01" process="test.process1" pParam1="value1"\n')
            txt_f.write('instance="tm1srv01" process="test.process2"\n')
            txt_f.flush()
            txt_path = txt_f.name

        # Create output path
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as json_f:
            json_path = json_f.name

        try:
            # Load and export (TaskfileSource created for potential future use)
            taskfile = convert_txt_to_json(txt_path)

            # Write to JSON
            with open(json_path, "w", encoding="utf-8") as f:
                import json as json_module

                json_module.dump(taskfile.to_dict(), f, indent=2)

            # Verify output
            self.assertTrue(os.path.exists(json_path))
            parsed = parse_json_taskfile(json_path)
            self.assertEqual(len(parsed.tasks), 2)
            self.assertEqual(parsed.tasks[0].process, "test.process1")
            self.assertEqual(parsed.tasks[0].parameters.get("pParam1"), "value1")

        finally:
            os.unlink(txt_path)
            if os.path.exists(json_path):
                os.unlink(json_path)

    def test_export_json_roundtrip(self):
        """Test that JSON export preserves all task properties"""
        # Create original JSON taskfile
        original_data = {
            "version": "2.0",
            "metadata": {"workflow": "test-roundtrip", "name": "Roundtrip Test"},
            "settings": {"max_workers": 4, "retries": 2},
            "tasks": [
                {
                    "id": "1",
                    "instance": "tm1srv01",
                    "process": "test.process1",
                    "parameters": {"pParam1": "value1"},
                    "stage": "Extract",
                    "safe_retry": True,
                    "timeout": 300,
                },
                {
                    "id": "2",
                    "instance": "tm1srv01",
                    "process": "test.process2",
                    "predecessors": ["1"],
                    "stage": "Transform",
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(original_data, f)
            f.flush()
            input_path = f.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_path = f.name

        try:
            # Load and re-export
            taskfile = parse_json_taskfile(input_path)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(taskfile.to_dict(), f, indent=2)

            # Parse the exported file
            exported = parse_json_taskfile(output_path)

            # Verify preservation
            self.assertEqual(exported.version, "2.0")
            self.assertEqual(exported.metadata.workflow, "test-roundtrip")
            self.assertEqual(exported.settings.max_workers, 4)
            self.assertEqual(len(exported.tasks), 2)

            # Check task 1
            task1 = exported.tasks[0]
            self.assertEqual(task1.id, "1")
            self.assertEqual(task1.stage, "Extract")
            self.assertTrue(task1.safe_retry)
            self.assertEqual(task1.timeout, 300)
            self.assertEqual(task1.parameters.get("pParam1"), "value1")

            # Check task 2
            task2 = exported.tasks[1]
            self.assertEqual(task2.predecessors, ["1"])
            self.assertEqual(task2.stage, "Transform")

        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_export_txt_with_wait_preserves_predecessors(self):
        """Test that TXT with wait markers exports with correct predecessors"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write('instance="tm1srv01" process="extract1"\n')
            f.write('instance="tm1srv01" process="extract2"\n')
            f.write("wait\n")
            f.write('instance="tm1srv01" process="transform1"\n')
            f.flush()
            txt_path = f.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_path = f.name

        try:
            # Convert TXT to JSON (this processes wait markers)
            taskfile = convert_txt_to_json(txt_path)

            # Export to JSON
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(taskfile.to_dict(), f, indent=2)

            # Verify
            exported = parse_json_taskfile(json_path)
            self.assertEqual(len(exported.tasks), 3)

            # Tasks after wait should have predecessors
            transform_task = next(t for t in exported.tasks if t.process == "transform1")
            self.assertTrue(len(transform_task.predecessors) > 0)

        finally:
            os.unlink(txt_path)
            if os.path.exists(json_path):
                os.unlink(json_path)


class TestArchiveTaskfile(unittest.TestCase):
    """Tests for taskfile archiving."""

    def _make_taskfile(self, workflow="test-workflow"):
        return Taskfile(
            metadata=TaskfileMetadata(workflow=workflow),
            tasks=[
                TaskDefinition(
                    id="1",
                    instance="tm1srv01",
                    process="process1",
                    parameters={"pRegion": "US"},
                ),
                TaskDefinition(
                    id="2",
                    instance="tm1srv01",
                    process="process2",
                    predecessors=["1"],
                ),
            ],
        )

    def test_archive_creates_file(self):
        """Test that archive creates the JSON file in the correct location."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with unittest.mock.patch(
                "rushti.utils.resolve_app_path",
                side_effect=lambda p: os.path.join(tmpdir, p),
            ):
                taskfile = self._make_taskfile()
                path = archive_taskfile(taskfile, "my_workflow", "20260216_143025")

                self.assertTrue(os.path.isfile(path))
                self.assertIn("archive", path)
                self.assertIn("my_workflow", path)
                self.assertTrue(path.endswith("20260216_143025.json"))

    def test_archive_contains_valid_json(self):
        """Test that archived file is valid JSON matching the taskfile."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with unittest.mock.patch(
                "rushti.utils.resolve_app_path",
                side_effect=lambda p: os.path.join(tmpdir, p),
            ):
                taskfile = self._make_taskfile()
                path = archive_taskfile(taskfile, "daily_etl", "20260216_143025")

                with open(path) as f:
                    data = json.load(f)

                self.assertEqual(data["metadata"]["workflow"], "test-workflow")
                self.assertEqual(len(data["tasks"]), 2)
                self.assertEqual(data["tasks"][0]["process"], "process1")
                self.assertEqual(data["tasks"][1]["predecessors"], ["1"])

    def test_archive_directory_structure(self):
        """Test that archive creates workflow subdirectory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with unittest.mock.patch(
                "rushti.utils.resolve_app_path",
                side_effect=lambda p: os.path.join(tmpdir, p),
            ):
                taskfile = self._make_taskfile()
                archive_taskfile(taskfile, "finance_close", "run1")
                archive_taskfile(taskfile, "finance_close", "run2")

                workflow_dir = os.path.join(tmpdir, "archive", "finance_close")
                self.assertTrue(os.path.isdir(workflow_dir))
                self.assertTrue(os.path.isfile(os.path.join(workflow_dir, "run1.json")))
                self.assertTrue(os.path.isfile(os.path.join(workflow_dir, "run2.json")))

    def test_archive_returns_absolute_path(self):
        """Test that the returned path is absolute."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with unittest.mock.patch(
                "rushti.utils.resolve_app_path",
                side_effect=lambda p: os.path.join(tmpdir, p),
            ):
                taskfile = self._make_taskfile()
                path = archive_taskfile(taskfile, "wf", "run1")
                self.assertTrue(os.path.isabs(path))


if __name__ == "__main__":
    unittest.main()
