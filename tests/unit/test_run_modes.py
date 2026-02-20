"""
Unit tests for run modes functionality.
Covers RunMode enum, validation, visualization, and task expansion.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

from rushti.taskfile_ops import (
    RunMode,
    ValidationResult,
    visualize_dag,
    validate_taskfile_full,
    _check_dag_cycles,
    _expand_task_parameters,
    _write_taskfile,
)
from rushti.taskfile import TaskDefinition, Taskfile
from rushti.task import Task, OptimizedTask
from rushti.dag import DAG


class TestRunModeEnum(unittest.TestCase):
    """Tests for RunMode enum"""

    def test_run_mode_values(self):
        """Test all RunMode enum values exist"""
        self.assertEqual(RunMode.RUN.value, "run")
        self.assertEqual(RunMode.EXPAND.value, "expand")
        self.assertEqual(RunMode.VISUALIZE.value, "visualize")
        self.assertEqual(RunMode.VALIDATE.value, "validate")
        self.assertEqual(RunMode.ANALYZE.value, "analyze")
        self.assertEqual(RunMode.BUILD.value, "build")

    def test_run_mode_count(self):
        """Test there are exactly 6 run modes"""
        self.assertEqual(len(RunMode), 6)


class TestValidationResult(unittest.TestCase):
    """Tests for ValidationResult dataclass"""

    def test_validation_result_defaults(self):
        """Test ValidationResult default values"""
        result = ValidationResult(valid=True)
        self.assertTrue(result.valid)
        self.assertEqual(result.errors, [])
        self.assertEqual(result.warnings, [])
        self.assertEqual(result.info, [])
        self.assertEqual(result.tm1_checks, {})

    def test_validation_result_with_errors(self):
        """Test ValidationResult with errors"""
        result = ValidationResult(
            valid=False,
            errors=["Error 1", "Error 2"],
            warnings=["Warning 1"],
            info=["Info 1"],
        )
        self.assertFalse(result.valid)
        self.assertEqual(len(result.errors), 2)
        self.assertEqual(len(result.warnings), 1)
        self.assertEqual(len(result.info), 1)

    def test_validation_result_to_dict(self):
        """Test ValidationResult.to_dict()"""
        result = ValidationResult(
            valid=True,
            errors=[],
            warnings=["Warning 1"],
            info=["Info 1", "Info 2"],
            tm1_checks={"instance1": {"connected": True}},
        )
        d = result.to_dict()
        self.assertTrue(d["valid"])
        self.assertEqual(d["errors"], [])
        self.assertEqual(d["warnings"], ["Warning 1"])
        self.assertEqual(len(d["info"]), 2)
        self.assertEqual(d["tm1_checks"]["instance1"]["connected"], True)

    def test_validation_result_to_json(self):
        """Test ValidationResult.to_json()"""
        result = ValidationResult(valid=False, errors=["Test error"])
        json_str = result.to_json()
        parsed = json.loads(json_str)
        self.assertFalse(parsed["valid"])
        self.assertIn("Test error", parsed["errors"])


class TestDAGCycleDetection(unittest.TestCase):
    """Tests for _check_dag_cycles helper function"""

    def test_no_cycles(self):
        """Test detection of valid DAG with no cycles"""
        tasks = [
            TaskDefinition(id="1", instance="tm1", process="p1", predecessors=[]),
            TaskDefinition(id="2", instance="tm1", process="p2", predecessors=["1"]),
            TaskDefinition(id="3", instance="tm1", process="p3", predecessors=["2"]),
        ]
        errors = _check_dag_cycles(tasks)
        self.assertEqual(errors, [])

    def test_simple_cycle(self):
        """Test detection of simple A -> B -> A cycle"""
        tasks = [
            TaskDefinition(id="1", instance="tm1", process="p1", predecessors=["2"]),
            TaskDefinition(id="2", instance="tm1", process="p2", predecessors=["1"]),
        ]
        errors = _check_dag_cycles(tasks)
        self.assertTrue(any("Circular dependency" in e for e in errors))

    def test_complex_cycle(self):
        """Test detection of A -> B -> C -> A cycle"""
        tasks = [
            TaskDefinition(id="1", instance="tm1", process="p1", predecessors=["3"]),
            TaskDefinition(id="2", instance="tm1", process="p2", predecessors=["1"]),
            TaskDefinition(id="3", instance="tm1", process="p3", predecessors=["2"]),
        ]
        errors = _check_dag_cycles(tasks)
        self.assertTrue(any("Circular dependency" in e for e in errors))

    def test_nonexistent_predecessor(self):
        """Test detection of references to non-existent predecessors"""
        tasks = [
            TaskDefinition(id="1", instance="tm1", process="p1", predecessors=["999"]),
        ]
        errors = _check_dag_cycles(tasks)
        self.assertTrue(any("non-existent predecessor '999'" in e for e in errors))

    def test_parallel_tasks_no_cycle(self):
        """Test parallel tasks with shared dependency don't trigger false positive"""
        tasks = [
            TaskDefinition(id="1", instance="tm1", process="p1", predecessors=[]),
            TaskDefinition(id="2", instance="tm1", process="p2", predecessors=["1"]),
            TaskDefinition(id="3", instance="tm1", process="p3", predecessors=["1"]),
            TaskDefinition(id="4", instance="tm1", process="p4", predecessors=["2", "3"]),
        ]
        errors = _check_dag_cycles(tasks)
        self.assertEqual(errors, [])


class TestVisualizeDagHTML(unittest.TestCase):
    """Tests for visualize_dag function generating HTML visualization"""

    def test_visualize_json_taskfile_html(self):
        """Test generating HTML visualization from JSON taskfile"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_content = {
                "version": "2.0",
                "tasks": [
                    {"id": "1", "instance": "tm1srv01", "process": "p1", "predecessors": []},
                    {"id": "2", "instance": "tm1srv01", "process": "p2", "predecessors": ["1"]},
                ],
            }
            json.dump(json_content, f)
            f.flush()
            input_path = f.name

        output_path = tempfile.mktemp(suffix=".html")

        try:
            result_path = visualize_dag(input_path, output_path)
            self.assertTrue(os.path.exists(result_path))
            # Check it's a valid HTML file
            with open(result_path, "r", encoding="utf-8") as f:
                content = f.read()
            self.assertIn("<!DOCTYPE html>", content)
            self.assertIn("vis-network", content)
        finally:
            os.unlink(input_path)
            if os.path.exists(result_path):
                os.unlink(result_path)

    def test_visualize_html_with_stages(self):
        """Test HTML visualization includes stage information"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_content = {
                "version": "2.0",
                "tasks": [
                    {"id": "1", "instance": "tm1", "process": "Process1", "stage": "extract"},
                    {
                        "id": "2",
                        "instance": "tm1",
                        "process": "Process2",
                        "stage": "load",
                        "predecessors": ["1"],
                    },
                ],
            }
            json.dump(json_content, f)
            f.flush()
            input_path = f.name

        output_path = tempfile.mktemp(suffix=".html")

        try:
            result_path = visualize_dag(input_path, output_path)
            with open(result_path, "r", encoding="utf-8") as f:
                html = f.read()

            self.assertIn("<!DOCTYPE html>", html)
            self.assertIn("Process1", html)
            self.assertIn("Process2", html)
            self.assertIn("extract", html)
            self.assertIn("load", html)
        finally:
            os.unlink(input_path)
            if os.path.exists(result_path):
                os.unlink(result_path)

    def test_visualize_html_no_edges(self):
        """Test HTML visualization with no edges"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_content = {
                "version": "2.0",
                "tasks": [
                    {"id": "1", "instance": "tm1", "process": "Process1"},
                ],
            }
            json.dump(json_content, f)
            f.flush()
            input_path = f.name

        output_path = tempfile.mktemp(suffix=".html")

        try:
            result_path = visualize_dag(input_path, output_path)
            with open(result_path, "r", encoding="utf-8") as f:
                html = f.read()
            self.assertIn("<!DOCTYPE html>", html)
            self.assertIn("Process1", html)
        finally:
            os.unlink(input_path)
            if os.path.exists(result_path):
                os.unlink(result_path)

    def test_visualize_html_with_long_label(self):
        """Test HTML visualization with long process names"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_content = {
                "version": "2.0",
                "tasks": [
                    {
                        "id": "1",
                        "instance": "tm1",
                        "process": "VeryLongProcessNameThatExceedsTwentyCharacters",
                    },
                ],
            }
            json.dump(json_content, f)
            f.flush()
            input_path = f.name

        output_path = tempfile.mktemp(suffix=".html")

        try:
            result_path = visualize_dag(input_path, output_path)
            self.assertTrue(os.path.exists(result_path))
        finally:
            os.unlink(input_path)
            if os.path.exists(result_path):
                os.unlink(result_path)

    def test_show_parameters_option(self):
        """Test show_parameters includes parameters in visualization"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_content = {
                "version": "2.0",
                "tasks": [
                    {
                        "id": "1",
                        "instance": "tm1",
                        "process": "TestProc",
                        "parameters": {"pYear": "2024"},
                    },
                ],
            }
            json.dump(json_content, f)
            f.flush()
            input_path = f.name

        output_path = tempfile.mktemp(suffix=".html")

        try:
            result_path = visualize_dag(input_path, output_path, show_parameters=True)
            with open(result_path, "r", encoding="utf-8") as f:
                html = f.read()
            self.assertIn("pYear", html)
            self.assertIn("2024", html)
        finally:
            os.unlink(input_path)
            if os.path.exists(result_path):
                os.unlink(result_path)

    def test_visualization_with_stages(self):
        """Test visualization with stage information"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_content = {
                "version": "2.0",
                "tasks": [
                    {"id": "1", "instance": "tm1", "process": "Proc1", "stage": "extract"},
                    {
                        "id": "2",
                        "instance": "tm1",
                        "process": "Proc2",
                        "stage": "load",
                        "predecessors": ["1"],
                    },
                ],
            }
            json.dump(json_content, f)
            f.flush()
            input_path = f.name

        output_path = tempfile.mktemp(suffix=".html")
        result_path = None

        try:
            result_path = visualize_dag(input_path, output_path)
            # Should generate an HTML file with stage information
            self.assertTrue(os.path.exists(result_path))
            with open(result_path, "r", encoding="utf-8") as f:
                html = f.read()
            # Verify stages are included in the HTML visualization
            self.assertIn("extract", html)
            self.assertIn("load", html)
        finally:
            os.unlink(input_path)
            if result_path and os.path.exists(result_path):
                os.unlink(result_path)


class TestValidateTaskfileFull(unittest.TestCase):
    """Tests for validate_taskfile_full function"""

    def test_validate_nonexistent_file(self):
        """Test validation of non-existent file"""
        result = validate_taskfile_full(
            "/nonexistent/path/tasks.json",
            "/nonexistent/config.ini",
            check_tm1=False,
        )
        self.assertFalse(result.valid)
        self.assertTrue(any("not found" in e.lower() for e in result.errors))

    def test_validate_valid_json_taskfile(self):
        """Test validation of valid JSON taskfile without TM1 checks"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_content = {
                "version": "2.0",
                "tasks": [
                    {"id": "1", "instance": "tm1srv01", "process": "p1", "predecessors": []},
                    {"id": "2", "instance": "tm1srv01", "process": "p2", "predecessors": ["1"]},
                ],
            }
            json.dump(json_content, f)
            f.flush()
            file_path = f.name

        try:
            result = validate_taskfile_full(file_path, "/nonexistent/config.ini", check_tm1=False)
            self.assertTrue(result.valid)
            self.assertTrue(any("File type: json" in i for i in result.info))
        finally:
            os.unlink(file_path)

    def test_validate_json_with_cycles(self):
        """Test validation catches circular dependencies"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_content = {
                "version": "2.0",
                "tasks": [
                    {"id": "1", "instance": "tm1srv01", "process": "p1", "predecessors": ["2"]},
                    {"id": "2", "instance": "tm1srv01", "process": "p2", "predecessors": ["1"]},
                ],
            }
            json.dump(json_content, f)
            f.flush()
            file_path = f.name

        try:
            result = validate_taskfile_full(file_path, "/nonexistent/config.ini", check_tm1=False)
            self.assertFalse(result.valid)
            self.assertTrue(any("Circular dependency" in e for e in result.errors))
        finally:
            os.unlink(file_path)

    def test_validate_invalid_json(self):
        """Test validation of invalid JSON file"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ invalid json }")
            f.flush()
            file_path = f.name

        try:
            result = validate_taskfile_full(file_path, "/nonexistent/config.ini", check_tm1=False)
            self.assertFalse(result.valid)
            self.assertTrue(any("JSON" in e or "Parse error" in e for e in result.errors))
        finally:
            os.unlink(file_path)


class TestExpandTaskParameters(unittest.TestCase):
    """Tests for _expand_task_parameters helper function"""

    def test_expand_single_parameter(self):
        """Test expanding a single parameter"""
        task = TaskDefinition(
            id="1",
            instance="tm1",
            process="test.process",
            parameters={"pElement*": "MDX_EXPRESSION"},
        )
        expansion_results = {"pElement*": ["Element1", "Element2", "Element3"]}

        expanded = _expand_task_parameters(task, expansion_results)

        self.assertEqual(len(expanded), 3)
        self.assertEqual(expanded[0].parameters["pElement"], "Element1")
        self.assertEqual(expanded[1].parameters["pElement"], "Element2")
        self.assertEqual(expanded[2].parameters["pElement"], "Element3")

    def test_expand_multiple_parameters(self):
        """Test expanding multiple parameters (cartesian product)"""
        task = TaskDefinition(
            id="1",
            instance="tm1",
            process="test.process",
            parameters={"pDim1*": "MDX1", "pDim2*": "MDX2"},
        )
        expansion_results = {
            "pDim1*": ["A", "B"],
            "pDim2*": ["X", "Y"],
        }

        expanded = _expand_task_parameters(task, expansion_results)

        # Should have 2 x 2 = 4 combinations
        self.assertEqual(len(expanded), 4)

    def test_expand_no_expansion(self):
        """Test task with no expansion needed returns original"""
        task = TaskDefinition(
            id="1",
            instance="tm1",
            process="test.process",
            parameters={"pRegular": "value"},
        )

        expanded = _expand_task_parameters(task, {})

        self.assertEqual(len(expanded), 1)
        self.assertEqual(expanded[0].parameters["pRegular"], "value")

    def test_expand_preserves_task_properties(self):
        """Test expanded tasks preserve original properties"""
        task = TaskDefinition(
            id="1",
            instance="tm1srv01",
            process="test.process",
            parameters={"pElement*": "MDX"},
            predecessors=["0"],
            stage="Extract",
            safe_retry=True,
            timeout=300,
        )
        expansion_results = {"pElement*": ["Element1"]}

        expanded = _expand_task_parameters(task, expansion_results)

        self.assertEqual(expanded[0].instance, "tm1srv01")
        self.assertEqual(expanded[0].process, "test.process")
        self.assertEqual(expanded[0].predecessors, ["0"])
        self.assertEqual(expanded[0].stage, "Extract")
        self.assertTrue(expanded[0].safe_retry)
        self.assertEqual(expanded[0].timeout, 300)


class TestWriteTaskfile(unittest.TestCase):
    """Tests for _write_taskfile helper function"""

    def test_write_json_taskfile(self):
        """Test writing taskfile to JSON format"""
        taskfile = Taskfile(
            version="2.0",
            tasks=[
                TaskDefinition(id="1", instance="tm1", process="p1"),
                TaskDefinition(id="2", instance="tm1", process="p2", predecessors=["1"]),
            ],
            settings={"max_workers": 8},
            metadata={"name": "Test"},
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_path = Path(f.name)

        try:
            _write_taskfile(taskfile, output_path, "json")
            self.assertTrue(output_path.exists())

            with open(output_path) as f:
                data = json.load(f)

            self.assertEqual(data["version"], "2.0")
            self.assertEqual(len(data["tasks"]), 2)
            self.assertEqual(data["settings"]["max_workers"], 8)
            self.assertEqual(data["metadata"]["name"], "Test")
        finally:
            if output_path.exists():
                os.unlink(output_path)

    def test_write_txt_format_not_implemented(self):
        """Test TXT format raises NotImplementedError"""
        taskfile = Taskfile(
            version="2.0",
            tasks=[TaskDefinition(id="1", instance="tm1", process="p1")],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        try:
            with self.assertRaises(NotImplementedError):
                _write_taskfile(taskfile, output_path, "txt")
        finally:
            if output_path.exists():
                os.unlink(output_path)


class TestStageOrdering(unittest.TestCase):
    """Tests for stage ordering in DAG"""

    def test_apply_stage_ordering(self):
        """Test that stage ordering adds dependencies between stages"""
        dag = DAG()
        # Create tasks with stages
        task1 = Task("tm1", "p1", {}, stage="Extract")
        task1.id = "1"
        task2 = Task("tm1", "p2", {}, stage="Extract")
        task2.id = "2"
        task3 = Task("tm1", "p3", {}, stage="Transform")
        task3.id = "3"
        task4 = Task("tm1", "p4", {}, stage="Load")
        task4.id = "4"

        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        dag.add_task(task4)

        # Apply stage ordering
        dag.apply_stage_ordering(["Extract", "Transform", "Load"])

        # Initially only Extract tasks should be ready
        ready = dag.get_ready_tasks()
        ready_ids = {str(t.id) for t in ready}
        self.assertEqual(ready_ids, {"1", "2"})

        # Mark Extract tasks complete
        dag.mark_running(task1)
        dag.mark_running(task2)
        dag.mark_complete("1", True)
        dag.mark_complete("2", True)

        # Now Transform should be ready
        ready = dag.get_ready_tasks()
        ready_ids = {str(t.id) for t in ready}
        self.assertEqual(ready_ids, {"3"})

    def test_apply_stage_ordering_empty(self):
        """Test that empty stage_order does nothing"""
        dag = DAG()
        task = Task("tm1", "p1", {})
        task.id = "1"
        dag.add_task(task)

        # Should not raise
        dag.apply_stage_ordering([])
        dag.apply_stage_ordering(None)


class TestExpandedTasksWithSameId(unittest.TestCase):
    """Tests for expanded tasks that share the same ID"""

    def test_multiple_instances_same_id_all_dispatched(self):
        """Test that all task instances with same ID are dispatched correctly.

        This tests the fix for the bug where only some instances of an expanded
        task were dispatched. When a task expands (e.g., via MDX expressions),
        all expanded instances share the same task ID. The DAG must dispatch
        all instances, even after some start running.
        """
        dag = DAG()

        # Create 5 task instances all with the same ID (simulating expansion)
        # Using OptimizedTask since that's what real expansion uses
        tasks = []
        for i in range(5):
            task = OptimizedTask(
                task_id="extract_1",  # All share same ID
                instance_name="tm1",
                process_name="process1",
                parameters={"pParam": str(i)},
                predecessors=[],
                require_predecessor_success=False,
            )
            tasks.append(task)
            dag.add_task(task)

        # Initially all 5 should be ready (no predecessors)
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 5)
        self.assertEqual({t.id for t in ready}, {"extract_1"})

        # Simulate dispatching 2 tasks (like with max_workers=2)
        dag.mark_running(tasks[0])
        dag.mark_running(tasks[1])

        # Remaining 3 should still be ready
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 3)

        # Complete the first 2
        dag.mark_complete(tasks[0], True)
        dag.mark_complete(tasks[1], True)

        # Still 3 remaining (not completed yet)
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 3)

        # Dispatch and complete the remaining 3
        for task in tasks[2:]:
            dag.mark_running(task)
        for task in tasks[2:]:
            dag.mark_complete(task, True)

        # No more tasks ready, DAG should be complete
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 0)
        self.assertTrue(dag.is_complete())

    def test_expanded_tasks_with_dependencies(self):
        """Test that dependent tasks wait for all instances of predecessor to complete.

        When task B depends on expanded task A (which has multiple instances),
        task B should only become ready after ALL instances of A complete.
        """
        dag = DAG()

        # Create 3 instances of "extract_1"
        extract_tasks = []
        for i in range(3):
            task = OptimizedTask(
                task_id="extract_1",
                instance_name="tm1",
                process_name="extract",
                parameters={"pNum": str(i)},
                predecessors=[],
                require_predecessor_success=False,
            )
            extract_tasks.append(task)
            dag.add_task(task)

        # Create a dependent task
        load_task = OptimizedTask(
            task_id="load_1",
            instance_name="tm1",
            process_name="load",
            parameters={},
            predecessors=["extract_1"],
            require_predecessor_success=False,
        )
        dag.add_task(load_task)

        # Initially only extract tasks are ready
        ready = dag.get_ready_tasks()
        ready_ids = {t.id for t in ready}
        self.assertEqual(ready_ids, {"extract_1"})
        self.assertEqual(len(ready), 3)

        # Complete 2 out of 3 extract tasks
        dag.mark_running(extract_tasks[0])
        dag.mark_running(extract_tasks[1])
        dag.mark_complete(extract_tasks[0], True)
        dag.mark_complete(extract_tasks[1], True)

        # load_1 should NOT be ready yet (1 extract still pending)
        ready = dag.get_ready_tasks()
        ready_ids = {t.id for t in ready}
        self.assertEqual(ready_ids, {"extract_1"})  # Only the remaining extract
        self.assertEqual(len(ready), 1)

        # Complete the last extract task
        dag.mark_running(extract_tasks[2])
        dag.mark_complete(extract_tasks[2], True)

        # Now load_1 should be ready
        ready = dag.get_ready_tasks()
        ready_ids = {t.id for t in ready}
        self.assertEqual(ready_ids, {"load_1"})
        self.assertEqual(len(ready), 1)


if __name__ == "__main__":
    unittest.main()
