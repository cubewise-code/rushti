"""Integration tests for the TM1 chore task kind (issue #156) on v11.

Targets a live TM1 v11 instance (``tm1srv01``) that has two preconfigured
chores:
- ``test_chore_success`` — succeeds (HTTP 204).
- ``test_chore_error``   — fails (HTTP 500, "Chore execution failed").

Both must be SingleCommit so the ``safe_retry`` path is exercisable.

Run with: ``pytest tests/integration/test_v11_chore_kind.py -v``
"""

import asyncio
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime

import pytest

_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)
_integration_path = os.path.dirname(os.path.abspath(__file__))
if _integration_path not in sys.path:
    sys.path.insert(0, _integration_path)

from rushti.execution import (
    ExecutionContext,
    execute_chore_with_retries,
    execute_task,
    logout,
    setup_tm1_services,
    validate_tasks,
    work_through_tasks_dag,
)
from rushti.parsing import convert_json_to_dag
from rushti.task import Task
from rushti.taskfile import (
    Taskfile,
    TaskfileMetadata,
    TaskfileSettings,
    TaskDefinition,
)
from rushti.tm1_build import build_logging_objects, verify_logging_objects
from rushti.tm1_integration import (
    build_results_dataframe,
    upload_results_to_tm1,
)
from rushti.stats import StatsDatabase
from conftest import get_all_test_tm1_configs, get_test_tm1_names
from tm1_setup import setup_tm1_test_objects

CHORE_SUCCESS = "test_chore_success"
CHORE_ERROR = "test_chore_error"


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestV11ChoreKind(unittest.TestCase):
    """End-to-end chore execution against a live TM1 v11 instance."""

    INSTANCE = "tm1srv01"

    @classmethod
    def setUpClass(cls):
        configs, source = get_all_test_tm1_configs()
        if not configs or cls.INSTANCE not in configs:
            cls._tm1_available = False
            return

        cls._config_path = source
        cls._test_dir = tempfile.mkdtemp()

        # Bootstrap the connection pool via a tiny dummy taskfile so we
        # reuse the production setup_tm1_services code path. We do NOT
        # execute this taskfile.
        bootstrap = os.path.join(cls._test_dir, "bootstrap.txt")
        with open(bootstrap, "w") as f:
            f.write(f'instance="{cls.INSTANCE}" process="}}bedrock.server.wait" pWaitSec="0"\n')

        try:
            cls._tm1_services, cls._preserve_connections = setup_tm1_services(
                max_workers=4,
                tasks_file_path=bootstrap,
                config_path=cls._config_path,
            )
            tm1_names = get_test_tm1_names()
            for tm1 in cls._tm1_services.values():
                setup_tm1_test_objects(tm1, **tm1_names)
            cls._tm1_available = cls.INSTANCE in cls._tm1_services
            cls._tm1_names = tm1_names
        except Exception as e:
            print(f"TM1 setup failed: {e}")
            cls._tm1_available = False
            cls._tm1_services = {}
            cls._preserve_connections = {}

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "_tm1_services", None):
            logout(cls._tm1_services, cls._preserve_connections or {})
        if hasattr(cls, "_test_dir"):
            shutil.rmtree(cls._test_dir, ignore_errors=True)

    def setUp(self):
        if not getattr(self, "_tm1_available", False):
            self.skipTest(f"{self.INSTANCE} not available")
        Task.reset_id_counter()
        # Verify the two test chores are present — if a future operator
        # tears them down, the suite should skip rather than spuriously
        # fail.
        tm1 = self._tm1_services[self.INSTANCE]
        for chore in (CHORE_SUCCESS, CHORE_ERROR):
            if not tm1.chores.exists(chore):
                self.skipTest(f"chore '{chore}' not present on {self.INSTANCE}")

    # ------------------------------------------------------------------
    # Direct retry helper
    # ------------------------------------------------------------------

    def test_execute_chore_with_retries_success(self):
        tm1 = self._tm1_services[self.INSTANCE]
        task = Task(instance_name=self.INSTANCE, chore_name=CHORE_SUCCESS)
        ok, status, log_file, attempt = execute_chore_with_retries(tm1, task, retries=2)
        self.assertTrue(ok)
        self.assertEqual(status, "Completed")
        self.assertEqual(log_file, "")
        self.assertEqual(attempt, 0)

    def test_execute_chore_with_retries_failure_no_safe_retry(self):
        tm1 = self._tm1_services[self.INSTANCE]
        task = Task(instance_name=self.INSTANCE, chore_name=CHORE_ERROR, safe_retry=False)
        with self.assertRaises(Exception) as cm:
            execute_chore_with_retries(tm1, task, retries=3)
        # safe_retry=False → single attempt regardless of the global cap.
        self.assertIn("Chore execution failed", str(cm.exception))

    def test_execute_chore_with_retries_failure_with_safe_retry(self):
        # safe_retry exhausts the retry budget and then raises. We don't
        # assert exact call counts here (TM1 doesn't expose them), but we
        # do assert the raised exception's text to be sure we went via
        # the chore path.
        tm1 = self._tm1_services[self.INSTANCE]
        task = Task(instance_name=self.INSTANCE, chore_name=CHORE_ERROR, safe_retry=True)
        with self.assertRaises(Exception) as cm:
            execute_chore_with_retries(tm1, task, retries=1)
        self.assertIn("Chore execution failed", str(cm.exception))

    # ------------------------------------------------------------------
    # validate_tasks chore branch
    # ------------------------------------------------------------------

    def test_validate_tasks_chore_exists(self):
        task = Task(instance_name=self.INSTANCE, chore_name=CHORE_SUCCESS)
        ok = validate_tasks([task], self._tm1_services)
        self.assertTrue(ok)

    def test_validate_tasks_chore_missing(self):
        task = Task(
            instance_name=self.INSTANCE,
            chore_name="this_chore_definitely_does_not_exist",
        )
        ok = validate_tasks([task], self._tm1_services)
        self.assertFalse(ok)

    def test_validate_tasks_single_commit_check_passes(self):
        # Both test chores are SingleCommit on this instance.
        task = Task(
            instance_name=self.INSTANCE,
            chore_name=CHORE_SUCCESS,
            safe_retry=True,
        )
        ok = validate_tasks([task], self._tm1_services)
        self.assertTrue(ok)

    # ------------------------------------------------------------------
    # execute_task dispatch
    # ------------------------------------------------------------------

    def test_execute_task_dispatches_chore_success(self):
        task = Task(instance_name=self.INSTANCE, chore_name=CHORE_SUCCESS)
        ctx = ExecutionContext()
        ok = execute_task(ctx, task, retries=0, tm1_services=self._tm1_services)
        self.assertTrue(ok)

    def test_execute_task_dispatches_chore_failure(self):
        task = Task(instance_name=self.INSTANCE, chore_name=CHORE_ERROR)
        ctx = ExecutionContext()
        ok = execute_task(ctx, task, retries=0, tm1_services=self._tm1_services)
        self.assertFalse(ok)

    # ------------------------------------------------------------------
    # Mixed process + chore DAG
    # ------------------------------------------------------------------

    def test_mixed_dag_executes_in_order(self):
        """Mixed process + chore taskfile end-to-end, predecessors crossing kinds.

        DAG: process (id=1) → chore_success (id=2) → process (id=3)
        """
        Task.reset_id_counter()
        taskfile = Taskfile(
            metadata=TaskfileMetadata(workflow="chore-mixed-v11"),
            settings=TaskfileSettings(),
            tasks=[
                TaskDefinition(
                    id="1",
                    instance=self.INSTANCE,
                    process="}bedrock.server.wait",
                    parameters={"pWaitSec": "0"},
                ),
                TaskDefinition(
                    id="2",
                    instance=self.INSTANCE,
                    chore=CHORE_SUCCESS,
                    predecessors=["1"],
                    require_predecessor_success=True,
                ),
                TaskDefinition(
                    id="3",
                    instance=self.INSTANCE,
                    process="}bedrock.server.wait",
                    parameters={"pWaitSec": "0"},
                    predecessors=["2"],
                    require_predecessor_success=True,
                ),
            ],
        )
        dag = convert_json_to_dag(taskfile)
        loop = asyncio.new_event_loop()
        try:
            outcomes = loop.run_until_complete(
                work_through_tasks_dag(
                    ExecutionContext(),
                    dag,
                    max_workers=2,
                    retries=0,
                    tm1_services=self._tm1_services,
                )
            )
        finally:
            loop.close()

        self.assertEqual(len(outcomes), 3)
        self.assertTrue(all(outcomes), f"Not all tasks succeeded: {outcomes}")

    def test_mixed_dag_failed_chore_aborts_dependent(self):
        """A failing chore must skip dependents that require predecessor success."""
        Task.reset_id_counter()
        taskfile = Taskfile(
            metadata=TaskfileMetadata(workflow="chore-mixed-fail-v11"),
            settings=TaskfileSettings(),
            tasks=[
                TaskDefinition(
                    id="1",
                    instance=self.INSTANCE,
                    chore=CHORE_ERROR,
                ),
                TaskDefinition(
                    id="2",
                    instance=self.INSTANCE,
                    process="}bedrock.server.wait",
                    parameters={"pWaitSec": "0"},
                    predecessors=["1"],
                    require_predecessor_success=True,
                ),
            ],
        )
        dag = convert_json_to_dag(taskfile)
        loop = asyncio.new_event_loop()
        try:
            outcomes = loop.run_until_complete(
                work_through_tasks_dag(
                    ExecutionContext(),
                    dag,
                    max_workers=2,
                    retries=0,
                    tm1_services=self._tm1_services,
                )
            )
        finally:
            loop.close()

        # Both tasks reported failure: chore_error explicitly, and the
        # dependent process aborts on require_predecessor_success.
        self.assertEqual(len(outcomes), 2)
        self.assertFalse(any(outcomes), f"Expected all failures, got {outcomes}")

    # ------------------------------------------------------------------
    # rushti.build idempotently adds the `chore` measure
    # ------------------------------------------------------------------

    def test_build_logging_objects_adds_chore_measure(self):
        tm1 = self._tm1_services[self.INSTANCE]
        names = self._tm1_names
        # Run idempotently; the rushti cube already exists from the
        # test environment setup. The additive merge must include
        # ``chore`` after this PR.
        build_logging_objects(tm1, force=False, **names)
        status = verify_logging_objects(tm1, **names)
        # The cube + dimensions all exist.
        self.assertTrue(status.get(names["cube_name"], False))

        # Verify the chore element landed in the measure dimension.
        elements = tm1.elements.get_element_names(
            dimension_name=names["dim_measure"],
            hierarchy_name=names["dim_measure"],
        )
        self.assertIn("chore", elements)
        self.assertIn("process", elements)

    # ------------------------------------------------------------------
    # Stats DB round-trip with chore tasks
    # ------------------------------------------------------------------

    def test_stats_db_records_chore_with_disjoint_signature(self):
        Task.reset_id_counter()
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            stats_db = StatsDatabase(db_path=db_path, enabled=True)
            run_id = "chore_stats_" + datetime.now().strftime("%Y%m%d_%H%M%S")
            workflow = "test-chore-stats-v11"
            stats_db.start_run(run_id=run_id, workflow=workflow)

            # Record one process row and one chore row sharing a name.
            start = datetime.now()
            stats_db.record_task(
                run_id=run_id,
                task_id="1",
                instance=self.INSTANCE,
                process="some.process",
                chore=None,
                parameters={},
                success=True,
                start_time=start,
                end_time=start,
                workflow=workflow,
            )
            stats_db.record_task(
                run_id=run_id,
                task_id="2",
                instance=self.INSTANCE,
                process="",
                chore="some.process",
                parameters=None,
                success=True,
                start_time=start,
                end_time=start,
                workflow=workflow,
            )

            rows = stats_db.get_run_results(run_id)
            self.assertEqual(len(rows), 2)
            sigs = {r["task_signature"] for r in rows}
            # Disjoint signature space — process and chore named the same
            # must NOT collide.
            self.assertEqual(len(sigs), 2)

            kinds = {("chore" if r.get("chore") else "process") for r in rows}
            self.assertEqual(kinds, {"process", "chore"})

            stats_db.complete_run(run_id=run_id, success_count=2, failure_count=0)
            stats_db.close()
        finally:
            os.unlink(db_path)

    # ------------------------------------------------------------------
    # Results-push round-trip: chore row lands under `chore` measure
    # ------------------------------------------------------------------

    def test_results_push_populates_chore_measure(self):
        """End-to-end: chore success → stats DB → CSV upload → load TI → cube cell.

        Exercises the full results pipeline that broke in the issue body:
        a chore-row's ``vchore`` variable must land in the ``chore``
        measure element under the right (workflow, run_id, task_id) tuple.
        """
        tm1 = self._tm1_services[self.INSTANCE]
        names = self._tm1_names
        workflow = "chore-resultspush-v11"
        # Use a fresh run_id so the cube cell doesn't collide with any
        # historical data and so we can assert exact-match retrieval.
        run_id = "chorerp_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        task_id = "1"

        # 1. Record a chore execution in the stats DB.
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            stats_db = StatsDatabase(db_path=db_path, enabled=True)
            stats_db.start_run(run_id=run_id, workflow=workflow)
            now = datetime.now()
            stats_db.record_task(
                run_id=run_id,
                task_id=task_id,
                instance=self.INSTANCE,
                process="",
                chore=CHORE_SUCCESS,
                parameters=None,
                success=True,
                start_time=now,
                end_time=now,
                workflow=workflow,
            )
            stats_db.complete_run(run_id=run_id, success_count=1, failure_count=0)

            # 2. Build the results DataFrame and upload it as the CSV that
            #    the ``}rushti.load.results`` TI consumes.
            df = build_results_dataframe(stats_db, workflow, run_id)
            self.assertFalse(df.empty)
            self.assertIn("chore", df.columns)
            self.assertEqual(df.iloc[0]["chore"], CHORE_SUCCESS)
            file_name = upload_results_to_tm1(tm1, workflow, run_id, df)

            # 3. Run the load process. It must (a) compile against the new
            #    vchore variable list, and (b) populate the ``chore``
            #    measure cell for this row. TM1 v11 stores uploaded
            #    files with a ``.blb`` extension internally; v12 keeps
            #    the original name.
            from TM1py.Utils import integerize_version

            version = integerize_version(tm1.version, 2)
            load_file_name = file_name + ".blb" if version < 12 else file_name
            success, status, error_file = tm1.processes.execute_with_return(
                process_name="}rushti.load.results",
                pSourceFile=load_file_name,
                pTargetCube=names["cube_name"],
                pWorkflow_Dim=names["dim_workflow"],
                pTaskId_Dim=names["dim_task"],
                pRunId_Dim=names["dim_run"],
            )
            self.assertTrue(
                success,
                f"}}rushti.load.results failed (status={status}); error file={error_file}",
            )

            # 4. Read the cube cell back and assert the chore name landed
            #    under the ``chore`` measure. TM1py's get_value() expects
            #    elements separated by ``,`` (positional, matched against
            #    the cube's dimension order).
            value = tm1.cells.get_value(
                cube_name=names["cube_name"],
                elements=",".join([workflow, run_id, task_id, "chore"]),
            )
            self.assertEqual(value, CHORE_SUCCESS)

            stats_db.close()
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
