"""Tests for the TM1 chore task kind (issue #156).

Covers the polymorphic Task abstraction:
- ``TaskDefinition`` mutual-exclusion + per-kind forbidden-field validation
- ``Task`` / ``OptimizedTask`` class-level mutual-exclusion invariant
- TXT → JSON conversion (chore keyword, validate_taskfile invocation,
  pre-existing malformed inputs now failing)
- Parsing: chore_name threaded through five construction sites;
  expand_task early-returns for chores
- Execution: dispatch to chore branch, retry on safe_retry, validation
  against a mocked TM1Service
- Cube reader: chore acceptance, both-populated rejection
- Signature: disjoint signature space, no collision with processes
"""

import os
import tempfile
import unittest
import unittest.mock
from unittest.mock import MagicMock

import pandas as pd

from rushti.execution import (
    ExecutionContext,
    execute_chore_with_retries,
    execute_task,
    validate_tasks,
)
from rushti.parsing import expand_task, extract_task_from_line, convert_json_to_dag
from rushti.stats.signature import calculate_task_signature
from rushti.task import OptimizedTask, Task
from rushti.taskfile import (
    TaskDefinition,
    Taskfile,
    TaskfileMetadata,
    TaskfileSettings,
    TaskfileValidationError,
    convert_txt_to_json,
    validate_taskfile,
)
from rushti.tm1_integration import _dataframe_to_task_definitions


def _base_chore_task(**overrides):
    task = {"id": "1", "instance": "tm1srv01", "chore": "daily_etl"}
    task.update(overrides)
    return {"version": "2.0", "tasks": [task]}


def _base_process_task(**overrides):
    task = {"id": "1", "instance": "tm1srv01", "process": "load.cube"}
    task.update(overrides)
    return {"version": "2.0", "tasks": [task]}


class TestKindValidation(unittest.TestCase):
    """validate_taskfile / validate_task with mixed kinds."""

    def test_valid_chore_task_passes(self):
        errors = validate_taskfile(_base_chore_task())
        self.assertEqual(errors, [])

    def test_chore_with_safe_retry_passes(self):
        errors = validate_taskfile(_base_chore_task(safe_retry=True))
        self.assertEqual(errors, [])

    def test_rejects_both_process_and_chore(self):
        errors = validate_taskfile(_base_chore_task(process="load.cube"))
        self.assertTrue(any("mutually exclusive" in e for e in errors), f"Got: {errors}")

    def test_rejects_neither_process_nor_chore(self):
        data = {
            "version": "2.0",
            "tasks": [{"id": "1", "instance": "tm1srv01"}],
        }
        errors = validate_taskfile(data)
        self.assertTrue(
            any("exactly one of 'process' or 'chore'" in e for e in errors),
            f"Got: {errors}",
        )

    def test_rejects_parameters_on_chore(self):
        errors = validate_taskfile(_base_chore_task(parameters={"x": "1"}))
        self.assertTrue(
            any("'parameters' is not allowed on chore tasks" in e for e in errors),
            f"Got: {errors}",
        )

    def test_rejects_succeed_on_minor_errors_on_chore(self):
        errors = validate_taskfile(_base_chore_task(succeed_on_minor_errors=True))
        self.assertTrue(
            any("'succeed_on_minor_errors' is not allowed on chore tasks" in e for e in errors),
            f"Got: {errors}",
        )

    def test_rejects_timeout_on_chore(self):
        errors = validate_taskfile(_base_chore_task(timeout=60))
        self.assertTrue(
            any("'timeout' is not allowed on chore tasks" in e for e in errors),
            f"Got: {errors}",
        )

    def test_rejects_cancel_at_timeout_on_chore(self):
        errors = validate_taskfile(_base_chore_task(cancel_at_timeout=True))
        self.assertTrue(
            any("'cancel_at_timeout' is not allowed on chore tasks" in e for e in errors),
            f"Got: {errors}",
        )

    def test_allows_default_empty_parameters_on_chore(self):
        # An explicit empty dict is silently accepted so round-tripping
        # a TaskDefinition's `to_dict()` output stays idempotent.
        errors = validate_taskfile(_base_chore_task(parameters={}))
        self.assertEqual(errors, [])


class TestTaskInvariant(unittest.TestCase):
    """Class-level mutual exclusion on Task.__init__."""

    def test_chore_only_task_constructs(self):
        Task.reset_id_counter()
        t = Task(instance_name="tm1srv01", chore_name="daily_etl")
        self.assertEqual(t.chore_name, "daily_etl")
        self.assertIsNone(t.process_name)

    def test_process_only_task_constructs(self):
        Task.reset_id_counter()
        t = Task(instance_name="tm1srv01", process_name="load.cube")
        self.assertEqual(t.process_name, "load.cube")
        self.assertIsNone(t.chore_name)

    def test_both_set_raises(self):
        Task.reset_id_counter()
        with self.assertRaises(ValueError):
            Task(
                instance_name="tm1srv01",
                process_name="load.cube",
                chore_name="daily_etl",
            )

    def test_neither_set_raises(self):
        Task.reset_id_counter()
        with self.assertRaises(ValueError):
            Task(instance_name="tm1srv01")

    def test_optimized_task_both_set_raises(self):
        with self.assertRaises(ValueError):
            OptimizedTask(
                task_id="1",
                instance_name="tm1srv01",
                process_name="load.cube",
                chore_name="daily_etl",
            )

    def test_translate_to_line_chore(self):
        Task.reset_id_counter()
        t = Task(instance_name="tm1srv01", chore_name="daily_etl", safe_retry=True)
        line = t.translate_to_line()
        self.assertIn('chore="daily_etl"', line)
        self.assertIn('instance="tm1srv01"', line)
        self.assertIn('safe_retry="True"', line)
        # Process-only fields must NOT appear.
        self.assertNotIn("succeed_on_minor_errors", line)
        self.assertNotIn("timeout", line)
        self.assertNotIn("cancel_at_timeout", line)

    def test_optimized_task_translate_to_line_chore(self):
        t = OptimizedTask(
            task_id="2",
            instance_name="tm1srv01",
            chore_name="daily_etl",
            predecessors=["1"],
            require_predecessor_success=True,
        )
        line = t.translate_to_line()
        self.assertIn('chore="daily_etl"', line)
        self.assertIn('predecessors="1"', line)
        self.assertNotIn("succeed_on_minor_errors", line)


class TestTxtConversion(unittest.TestCase):
    """convert_txt_to_json now invokes validate_taskfile."""

    def _write(self, content: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".txt")
        with os.fdopen(fd, "w") as f:
            f.write(content)
        self.addCleanup(os.unlink, path)
        return path

    def test_chore_line_parses(self):
        path = self._write('instance="tm1srv01" chore="daily_etl" safe_retry="true"\n')
        taskfile = convert_txt_to_json(path)
        self.assertEqual(len(taskfile.tasks), 1)
        self.assertEqual(taskfile.tasks[0].chore, "daily_etl")
        self.assertIsNone(taskfile.tasks[0].process)
        self.assertTrue(taskfile.tasks[0].safe_retry)

    def test_chore_with_forbidden_parameters_rejected(self):
        # The new validate_taskfile call exposes a previously-silent gap.
        path = self._write('instance="tm1srv01" chore="daily_etl" pX="1"\n')
        with self.assertRaises(TaskfileValidationError):
            convert_txt_to_json(path)

    def test_malformed_txt_now_raises(self):
        # Previously: silently produced a Task with empty instance/process.
        # Now: validate_taskfile rejects it explicitly.
        path = self._write('process="orphan.process"\n')
        with self.assertRaises(TaskfileValidationError):
            convert_txt_to_json(path)


class TestExpandTaskChore(unittest.TestCase):
    """expand_task early-returns for chore tasks."""

    def test_chore_task_short_circuits(self):
        Task.reset_id_counter()
        t = Task(instance_name="tm1srv01", chore_name="daily_etl")
        # tm1_services is empty — if expand_task tried to do anything
        # real with it for a chore, this would KeyError.
        result = expand_task(tm1_services={}, task=t)
        self.assertEqual(result, [t])


class TestParsingChoreThreading(unittest.TestCase):
    """All five Task construction sites thread chore_name."""

    def test_extract_task_from_line_chore(self):
        Task.reset_id_counter()
        task = extract_task_from_line('instance="tm1srv01" chore="daily_etl"', task_class=Task)
        self.assertEqual(task.chore_name, "daily_etl")
        self.assertIsNone(task.process_name)

    def test_extract_optimized_task_from_line_chore(self):
        task = extract_task_from_line(
            'id="1" instance="tm1srv01" chore="daily_etl"',
            task_class=OptimizedTask,
        )
        self.assertEqual(task.chore_name, "daily_etl")
        self.assertEqual(task.id, "1")

    def test_convert_json_to_dag_chore(self):
        taskfile = Taskfile(
            metadata=TaskfileMetadata(workflow="mixed"),
            settings=TaskfileSettings(),
            tasks=[
                TaskDefinition(id="1", instance="tm1srv01", chore="daily_etl"),
                TaskDefinition(
                    id="2",
                    instance="tm1srv01",
                    process="load.cube",
                    predecessors=["1"],
                ),
            ],
        )
        dag = convert_json_to_dag(taskfile)
        all_tasks = dag.get_all_tasks()
        chore_tasks = [t for t in all_tasks if t.chore_name]
        process_tasks = [t for t in all_tasks if t.process_name]
        self.assertEqual(len(chore_tasks), 1)
        self.assertEqual(len(process_tasks), 1)
        self.assertEqual(chore_tasks[0].chore_name, "daily_etl")


class TestSignatureChore(unittest.TestCase):
    """Chore signatures are disjoint from process signatures."""

    def test_chore_signature_ignores_parameters(self):
        sig_no_params = calculate_task_signature("tm1srv01", None, None, chore="daily_etl")
        sig_with_params = calculate_task_signature(
            "tm1srv01", None, {"ignored": "1"}, chore="daily_etl"
        )
        self.assertEqual(sig_no_params, sig_with_params)

    def test_chore_signature_disjoint_from_process(self):
        # Same instance, same name — but different kinds must hash differently.
        process_sig = calculate_task_signature("tm1srv01", "daily_etl", None)
        chore_sig = calculate_task_signature("tm1srv01", None, None, chore="daily_etl")
        self.assertNotEqual(process_sig, chore_sig)

    def test_different_chore_names_differ(self):
        sig_a = calculate_task_signature("tm1srv01", None, None, chore="etl_a")
        sig_b = calculate_task_signature("tm1srv01", None, None, chore="etl_b")
        self.assertNotEqual(sig_a, sig_b)


class TestExecuteChoreRetries(unittest.TestCase):
    """execute_chore_with_retries success + retry semantics."""

    def _chore_task(self, safe_retry=False):
        Task.reset_id_counter()
        return Task(
            instance_name="tm1srv01",
            chore_name="daily_etl",
            safe_retry=safe_retry,
        )

    def test_success_first_attempt(self):
        tm1 = MagicMock()
        tm1.chores.execute_chore = MagicMock(return_value=None)
        ok, status, log, attempt = execute_chore_with_retries(tm1, self._chore_task(), retries=3)
        self.assertTrue(ok)
        self.assertEqual(status, "Completed")
        self.assertEqual(log, "")
        self.assertEqual(attempt, 0)
        tm1.chores.execute_chore.assert_called_once_with("daily_etl")

    def test_failure_without_safe_retry_does_not_retry(self):
        tm1 = MagicMock()
        tm1.chores.execute_chore = MagicMock(side_effect=RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            execute_chore_with_retries(tm1, self._chore_task(safe_retry=False), retries=3)
        # safe_retry=False → single attempt regardless of global retries.
        self.assertEqual(tm1.chores.execute_chore.call_count, 1)

    def test_failure_with_safe_retry_retries(self):
        tm1 = MagicMock()
        tm1.chores.execute_chore = MagicMock(side_effect=RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            execute_chore_with_retries(tm1, self._chore_task(safe_retry=True), retries=2)
        # safe_retry=True → initial attempt + 2 retries = 3 calls.
        self.assertEqual(tm1.chores.execute_chore.call_count, 3)

    def test_safe_retry_recovers_on_second_attempt(self):
        tm1 = MagicMock()
        tm1.chores.execute_chore = MagicMock(side_effect=[RuntimeError("flake"), None])
        ok, _, _, attempt = execute_chore_with_retries(
            tm1, self._chore_task(safe_retry=True), retries=3
        )
        self.assertTrue(ok)
        self.assertEqual(attempt, 1)


class TestExecuteTaskDispatch(unittest.TestCase):
    """execute_task routes chore-kind tasks to the chore branch."""

    def test_chore_dispatch(self):
        Task.reset_id_counter()
        task = Task(instance_name="tm1srv01", chore_name="daily_etl")
        tm1 = MagicMock()
        tm1.chores.execute_chore = MagicMock(return_value=None)
        tm1_services = {"tm1srv01": tm1}
        ctx = ExecutionContext()

        result = execute_task(ctx, task, retries=0, tm1_services=tm1_services)
        self.assertTrue(result)
        tm1.chores.execute_chore.assert_called_once_with("daily_etl")
        # The process path must not have been touched.
        tm1.processes.execute_with_return.assert_not_called()


class TestValidateTasksChore(unittest.TestCase):
    """validate_tasks chore branch: existence + SINGLE_COMMIT check."""

    def _chore_task(self, safe_retry=False):
        Task.reset_id_counter()
        return Task(
            instance_name="tm1srv01",
            chore_name="daily_etl",
            safe_retry=safe_retry,
        )

    def test_existence_check(self):
        tm1 = MagicMock()
        tm1.chores.exists = MagicMock(return_value=True)
        tm1_services = {"tm1srv01": tm1}

        ok = validate_tasks([self._chore_task()], tm1_services)
        self.assertTrue(ok)
        tm1.chores.exists.assert_called_once_with("daily_etl")
        # No safe_retry → never fetched the chore object.
        tm1.chores.get.assert_not_called()

    def test_missing_chore_fails(self):
        tm1 = MagicMock()
        tm1.chores.exists = MagicMock(return_value=False)
        tm1_services = {"tm1srv01": tm1}

        ok = validate_tasks([self._chore_task()], tm1_services)
        self.assertFalse(ok)

    def test_safe_retry_requires_single_commit(self):
        from TM1py.Objects import Chore

        tm1 = MagicMock()
        tm1.chores.exists = MagicMock(return_value=True)
        bad_chore = MagicMock()
        bad_chore.execution_mode = Chore.MULTIPLE_COMMIT
        tm1.chores.get = MagicMock(return_value=bad_chore)

        ok = validate_tasks(
            [self._chore_task(safe_retry=True)],
            {"tm1srv01": tm1},
        )
        self.assertFalse(ok)
        tm1.chores.get.assert_called_once_with("daily_etl")

    def test_safe_retry_single_commit_ok(self):
        from TM1py.Objects import Chore

        tm1 = MagicMock()
        tm1.chores.exists = MagicMock(return_value=True)
        good_chore = MagicMock()
        good_chore.execution_mode = Chore.SINGLE_COMMIT
        tm1.chores.get = MagicMock(return_value=good_chore)

        ok = validate_tasks(
            [self._chore_task(safe_retry=True)],
            {"tm1srv01": tm1},
        )
        self.assertTrue(ok)


class TestCubeReaderChore(unittest.TestCase):
    """_dataframe_to_task_definitions accepts chore rows."""

    def _row(self, **kwargs):
        row = {
            "rushti_task_id": "1",
            "instance": "",
            "process": "",
            "chore": "",
            "parameters": "",
            "wait": "",
            "predecessors": "",
            "stage": "",
            "safe_retry": "",
            "timeout": "",
            "cancel_at_timeout": "",
            "require_predecessor_success": "",
            "succeed_on_minor_errors": "",
        }
        row.update(kwargs)
        return row

    def test_reads_chore_row(self):
        df = pd.DataFrame([self._row(rushti_task_id="1", instance="tm1srv01", chore="daily_etl")])
        tasks = _dataframe_to_task_definitions(df, mode="opt")
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].chore, "daily_etl")
        self.assertIsNone(tasks[0].process)

    def test_rejects_both_populated(self):
        df = pd.DataFrame(
            [
                self._row(
                    rushti_task_id="1",
                    instance="tm1srv01",
                    process="load.cube",
                    chore="daily_etl",
                )
            ]
        )
        with self.assertRaises(ValueError):
            _dataframe_to_task_definitions(df, mode="opt")


if __name__ == "__main__":
    unittest.main()
