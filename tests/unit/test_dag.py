"""Unit tests for DAG data structures and operations.

Tests for:
- DAG class methods
- Circular dependency detection
- Norm to DAG conversion
- Opt to DAG conversion
- Build DAG functionality
"""

import os
import sys
import unittest

# Path setup handled by conftest.py, but also support direct execution
_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

# Get the project root directory for test resources
_tests_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_project_root = os.path.dirname(_tests_dir)

from rushti.parsing import (  # noqa: E402
    extract_tasks_from_file_type_opt,
    build_dag,
)
from rushti.task import Wait, Task  # noqa: E402
from rushti.dag import (  # noqa: E402
    DAG,
    CircularDependencyError,
    convert_norm_to_dag,
    convert_opt_to_dag,
)


class TestDAGDataStructure(unittest.TestCase):
    """Tests for the DAG class methods"""

    def test_add_task(self):
        """Test adding tasks to a DAG"""
        dag = DAG()
        task = Task("tm1srv01", "process1", {"param": "value"})
        dag.add_task(task)

        self.assertEqual(len(dag), 1)
        self.assertEqual(dag.get_all_tasks(), [task])

    def test_add_dependency(self):
        """Test adding dependencies between tasks"""
        dag = DAG()
        task1 = Task("tm1srv01", "process1", {})
        task1.id = "1"
        task2 = Task("tm1srv01", "process2", {})
        task2.id = "2"

        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_dependency("2", "1")  # task2 depends on task1

        # Task1 should be ready (no predecessors)
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 1)
        self.assertEqual(str(ready[0].id), "1")

    def test_get_ready_tasks_initial(self):
        """Test get_ready_tasks returns tasks with no predecessors"""
        dag = DAG()
        task1 = Task("tm1srv01", "process1", {})
        task1.id = "1"
        task2 = Task("tm1srv01", "process2", {})
        task2.id = "2"
        task3 = Task("tm1srv01", "process3", {})
        task3.id = "3"

        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        dag.add_dependency("3", "1")  # task3 depends on task1
        dag.add_dependency("3", "2")  # task3 also depends on task2

        ready = dag.get_ready_tasks()
        ready_ids = {str(t.id) for t in ready}
        self.assertEqual(ready_ids, {"1", "2"})

    def test_mark_complete_success(self):
        """Test marking a task as completed successfully"""
        dag = DAG()
        task1 = Task("tm1srv01", "process1", {})
        task1.id = "1"
        task2 = Task("tm1srv01", "process2", {})
        task2.id = "2"

        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_dependency("2", "1")

        # Execute task1
        dag.mark_running(task1)
        dag.mark_complete(task1, True)

        # Now task2 should be ready
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 1)
        self.assertEqual(str(ready[0].id), "2")

    def test_mark_complete_failure(self):
        """Test marking a task as failed"""
        dag = DAG()
        task1 = Task("tm1srv01", "process1", {})
        task1.id = "1"

        dag.add_task(task1)
        dag.mark_running(task1)
        dag.mark_complete(task1, False)

        self.assertEqual(dag.get_task_result("1"), False)

    def test_is_complete(self):
        """Test is_complete returns True when all tasks done"""
        dag = DAG()
        task1 = Task("tm1srv01", "process1", {})
        task1.id = "1"

        dag.add_task(task1)
        self.assertFalse(dag.is_complete())

        dag.mark_running(task1)
        dag.mark_complete(task1, True)
        self.assertTrue(dag.is_complete())

    def test_expanded_tasks_wait_for_all_to_complete(self):
        """Test that expanded tasks with same ID all complete before successor runs.

        When a task ID expands to multiple instances (e.g., task "1" becomes 3 tasks),
        a dependent task (e.g., task "2" with predecessor "1") should only become
        ready after ALL instances of task "1" have completed.

        This tests the fix for the bug where dispatched_instances and completed_instances
        were conflated, causing successors to run too early.
        """
        dag = DAG()

        # Create 3 expanded tasks with same ID "1"
        task1a = Task("tm1srv01", "process1", {"param": "a"})
        task1a.id = "1"
        task1b = Task("tm1srv01", "process1", {"param": "b"})
        task1b.id = "1"
        task1c = Task("tm1srv01", "process1", {"param": "c"})
        task1c.id = "1"

        # Create task "2" that depends on "1"
        task2 = Task("tm1srv01", "process2", {})
        task2.id = "2"

        dag.add_task(task1a)
        dag.add_task(task1b)
        dag.add_task(task1c)
        dag.add_task(task2)
        dag.add_dependency("2", "1")

        # Initially, all 3 instances of "1" should be ready, but not "2"
        ready = dag.get_ready_tasks()
        ready_ids = [str(t.id) for t in ready]
        self.assertEqual(len(ready), 3)
        self.assertTrue(all(tid == "1" for tid in ready_ids))

        # Start ALL 3 instances (simulating parallel dispatch)
        dag.mark_running(task1a)
        dag.mark_running(task1b)
        dag.mark_running(task1c)

        # No more "1" instances should be ready (all dispatched)
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 0)  # Nothing ready - "1" running, "2" blocked

        # Complete first instance - "2" should NOT be ready yet
        dag.mark_complete(task1a, True)
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 0)  # Still nothing - "1" not fully complete

        # Complete second instance - "2" should STILL NOT be ready
        dag.mark_complete(task1b, True)
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 0)  # Still nothing - "1" not fully complete

        # Complete third instance - NOW "2" should be ready
        dag.mark_complete(task1c, True)
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 1)
        self.assertEqual(str(ready[0].id), "2")

    def test_expanded_tasks_partial_failure(self):
        """Test that expanded task failure is aggregated correctly.

        When any expanded task instance fails, the overall task ID should be
        marked as failed, affecting successors that require predecessor success.
        """
        dag = DAG()

        # Create 2 expanded tasks with same ID "1"
        task1a = Task("tm1srv01", "process1", {"param": "a"})
        task1a.id = "1"
        task1b = Task("tm1srv01", "process1", {"param": "b"})
        task1b.id = "1"

        dag.add_task(task1a)
        dag.add_task(task1b)

        # First task succeeds
        dag.mark_running(task1a)
        dag.mark_complete(task1a, True)

        # Second task fails
        dag.mark_running(task1b)
        dag.mark_complete(task1b, False)

        # Overall result should be False (aggregated with AND)
        self.assertEqual(dag.get_task_result("1"), False)


class TestCircularDependencyDetection(unittest.TestCase):
    """Tests for circular dependency detection"""

    def test_simple_cycle(self):
        """Test detection of simple A -> B -> A cycle"""
        dag = DAG()
        task1 = Task("tm1srv01", "process1", {})
        task1.id = "1"
        task2 = Task("tm1srv01", "process2", {})
        task2.id = "2"

        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_dependency("2", "1")  # 2 depends on 1
        dag.add_dependency("1", "2")  # 1 depends on 2 (creates cycle)

        with self.assertRaises(CircularDependencyError) as context:
            dag.validate()

        self.assertIn("Circular dependency detected", str(context.exception))

    def test_complex_cycle(self):
        """Test detection of A -> B -> C -> A cycle"""
        dag = DAG()
        task1 = Task("tm1srv01", "process1", {})
        task1.id = "1"
        task2 = Task("tm1srv01", "process2", {})
        task2.id = "2"
        task3 = Task("tm1srv01", "process3", {})
        task3.id = "3"

        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        dag.add_dependency("2", "1")  # 2 depends on 1
        dag.add_dependency("3", "2")  # 3 depends on 2
        dag.add_dependency("1", "3")  # 1 depends on 3 (creates cycle)

        with self.assertRaises(CircularDependencyError) as context:
            dag.validate()

        self.assertIn("Circular dependency detected", str(context.exception))

    def test_no_cycle(self):
        """Test validation passes for valid DAG"""
        dag = DAG()
        task1 = Task("tm1srv01", "process1", {})
        task1.id = "1"
        task2 = Task("tm1srv01", "process2", {})
        task2.id = "2"
        task3 = Task("tm1srv01", "process3", {})
        task3.id = "3"

        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        dag.add_dependency("2", "1")  # 2 depends on 1
        dag.add_dependency("3", "2")  # 3 depends on 2

        # Should not raise
        dag.validate()

    def test_cycle_path_in_error(self):
        """Test that cycle path is included in error message"""
        dag = DAG()
        task1 = Task("tm1srv01", "process1", {})
        task1.id = "A"
        task2 = Task("tm1srv01", "process2", {})
        task2.id = "B"

        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_dependency("B", "A")
        dag.add_dependency("A", "B")

        with self.assertRaises(CircularDependencyError) as context:
            dag.validate()

        # Check that the cycle path contains the task IDs
        error = context.exception
        self.assertTrue(len(error.cycle_path) >= 2)


class TestNormToDagConversion(unittest.TestCase):
    """Tests for norm mode to DAG conversion"""

    def test_single_sequence(self):
        """Test conversion of tasks without waits"""
        # Reset Task.id counter for consistent testing
        Task.id = 1

        task1 = Task("tm1srv01", "process1", {})
        task2 = Task("tm1srv01", "process2", {})

        tasks_and_waits = [task1, task2]
        dag = convert_norm_to_dag(tasks_and_waits)

        self.assertEqual(len(dag), 2)
        # Both tasks should be ready (same sequence)
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 2)

    def test_two_sequences_with_wait(self):
        """Test conversion with wait between sequences"""
        Task.id = 1

        task1 = Task("tm1srv01", "process1", {})
        task2 = Task("tm1srv01", "process2", {})
        wait = Wait()
        task3 = Task("tm1srv01", "process3", {})
        task4 = Task("tm1srv01", "process4", {})

        tasks_and_waits = [task1, task2, wait, task3, task4]
        dag = convert_norm_to_dag(tasks_and_waits)

        self.assertEqual(len(dag), 4)

        # Task1 and Task2 should be ready initially
        ready = dag.get_ready_tasks()
        ready_ids = {str(t.id) for t in ready}
        self.assertEqual(ready_ids, {"1", "2"})

        # After completing task1 and task2, task3 and task4 should be ready
        for t in ready:
            dag.mark_running(t)
            dag.mark_complete(str(t.id), True)

        ready = dag.get_ready_tasks()
        ready_ids = {str(t.id) for t in ready}
        self.assertEqual(ready_ids, {"3", "4"})

    def test_three_sequences(self):
        """Test conversion with multiple waits"""
        Task.id = 1

        task1 = Task("tm1srv01", "process1", {})
        wait1 = Wait()
        task2 = Task("tm1srv01", "process2", {})
        wait2 = Wait()
        task3 = Task("tm1srv01", "process3", {})

        tasks_and_waits = [task1, wait1, task2, wait2, task3]
        dag = convert_norm_to_dag(tasks_and_waits)

        self.assertEqual(len(dag), 3)

        # Only task1 should be ready initially
        ready = dag.get_ready_tasks()
        self.assertEqual(len(ready), 1)
        self.assertEqual(str(ready[0].id), "1")


class TestOptToDagConversion(unittest.TestCase):
    """Tests for opt mode to DAG conversion"""

    def test_simple_opt_conversion(self):
        """Test conversion of optimized tasks without explicit dependencies"""
        task_file = os.path.join(_project_root, "tests", "resources", "tasks_opt_happy_case.txt")
        tasks = extract_tasks_from_file_type_opt(task_file)
        dag = convert_opt_to_dag(tasks)
        self.assertGreater(len(dag), 0)

    def test_opt_conversion_with_dependencies(self):
        """Test conversion of optimized tasks with explicit dependencies"""
        task_file = os.path.join(_project_root, "tests", "resources", "tasks_opt_case1.txt")
        tasks = extract_tasks_from_file_type_opt(task_file)
        dag = convert_opt_to_dag(tasks)
        self.assertGreater(len(dag), 0)


class TestBuildDag(unittest.TestCase):
    """Tests for build_dag function"""

    def test_build_dag_opt_mode(self):
        """Test building DAG in optimized mode (auto-detected from file)"""
        task_file = os.path.join(_project_root, "tests", "resources", "tasks_opt_happy_case.txt")
        dag = build_dag(task_file, expand=False, tm1_services={})
        self.assertGreater(len(dag), 0)

    def test_build_dag_validates(self):
        """Test that build_dag validates the resulting DAG"""
        task_file = os.path.join(_project_root, "tests", "resources", "tasks_opt_case1.txt")
        dag = build_dag(task_file, expand=False, tm1_services={})
        # Should not raise - DAG is valid
        self.assertGreater(len(dag), 0)


if __name__ == "__main__":
    unittest.main()
