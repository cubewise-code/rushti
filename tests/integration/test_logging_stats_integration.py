"""Integration tests for logging and stats modules with SQLite database.

Tests the integration between execution logging and stats database to verify
that task execution results are properly logged and stored for analysis.
"""

import os
import tempfile
import unittest
from datetime import datetime, timedelta

from rushti.logging import (
    ExecutionLogger,
    FileLogDestination,
    TaskExecutionLog,
    create_execution_logger,
)
from rushti.stats import StatsDatabase


class TestLoggingStatsIntegration(unittest.TestCase):
    """Integration tests for logging and stats working together."""

    def setUp(self):
        """Create temporary database and logging setup."""
        self.temp_db = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".db")
        self.temp_db.close()
        self.db_path = self.temp_db.name

        # Create stats database
        self.stats_db = StatsDatabase(db_path=self.db_path, enabled=True)

    def tearDown(self):
        """Clean up temporary files."""
        self.stats_db.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_file_log_destination_writes_logs(self):
        """Test FileLogDestination logs execution results."""
        from rushti.logging import ExecutionRun

        destination = FileLogDestination()

        # Create execution run with logs
        run = ExecutionRun(
            run_id="20240101_100000", workflow="test-taskfile", start_time=datetime.now()
        )

        # Add task logs
        log1 = TaskExecutionLog(
            workflow="test-taskfile",
            task_id="task1",
            instance="tm1srv01",
            process="test.proc",
            parameters="{}",
            status="Success",
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(seconds=5),
            duration_seconds=5.0,
            retry_count=0,
            error_message=None,
        )
        run.add_log(log1)

        # Write logs
        success = destination.write_logs(run)

        self.assertTrue(success)

    def test_execution_logger_full_workflow(self):
        """Test ExecutionLogger end-to-end workflow."""
        destinations = [
            FileLogDestination(),
        ]

        logger = ExecutionLogger(workflow="integration-test", destinations=destinations)

        # Log multiple task executions
        for i in range(1, 4):
            start_time = datetime.now()
            end_time = start_time + timedelta(seconds=i)

            logger.log_task_execution(
                task_id=f"task{i}",
                instance="tm1srv01",
                process=f"Process{i}",
                parameters={"param": f"value{i}"},
                success=True,
                start_time=start_time,
                end_time=end_time,
                retry_count=0,
            )

        # Flush logs
        success = logger.flush()

        self.assertTrue(success)
        self.assertEqual(logger.log_count, 3)

    def test_create_execution_logger_factory(self):
        """Test create_execution_logger factory function."""
        logger = create_execution_logger(
            workflow="factory-test",
        )

        self.assertIsInstance(logger, ExecutionLogger)
        self.assertEqual(logger.workflow, "factory-test")

        # Log a task
        logger.log_task_execution(
            task_id="task1",
            instance="tm1srv01",
            process="test.proc",
            parameters={},
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(seconds=1),
            retry_count=0,
        )

        # Flush
        success = logger.flush()
        self.assertTrue(success)

    def test_stats_database_stores_execution_results(self):
        """Test StatsDatabase properly stores execution results."""
        start_time = datetime.now()
        run_id = "20240101_120000"

        # Start run
        self.stats_db.start_run(run_id=run_id, workflow="stats-test")

        # Record multiple tasks
        for i in range(1, 6):
            self.stats_db.record_task(
                run_id=run_id,
                task_id=f"task{i}",
                instance="tm1srv01",
                process=f"Process{i}",
                parameters={"index": str(i)},
                success=(i % 2 == 0),
                start_time=start_time + timedelta(seconds=i),
                end_time=start_time + timedelta(seconds=i + 1),
                retry_count=0,
                error_message="Error" if i % 2 != 0 else None,
                workflow="stats-test",
            )

        # Complete run
        self.stats_db.complete_run(run_id=run_id, success_count=2, failure_count=3)

        # Verify results were stored
        results = self.stats_db.get_run_results(run_id)

        self.assertEqual(len(results), 5)
        self.assertEqual(sum(1 for r in results if r["status"] == "Success"), 2)
        self.assertEqual(sum(1 for r in results if r["status"] == "Fail"), 3)

    def test_stats_database_task_history(self):
        """Test StatsDatabase stores task history for repeated executions."""
        workflow = "history-test"
        task_id = "repeated-task"
        instance = "tm1srv01"
        process = "test.proc"
        parameters = {"p1": "v1"}

        # Record same task multiple times with different durations
        durations = [10.0, 12.0, 11.0, 13.0, 12.5]

        for i, duration in enumerate(durations):
            run_id = f"run{i+1}"
            start_time = datetime.now() + timedelta(hours=i)

            self.stats_db.start_run(run_id=run_id, workflow=workflow)

            self.stats_db.record_task(
                run_id=run_id,
                task_id=task_id,
                instance=instance,
                process=process,
                parameters=parameters,
                success=True,
                start_time=start_time,
                end_time=start_time + timedelta(seconds=duration),
                retry_count=0,
                error_message=None,
                workflow=workflow,
            )

            self.stats_db.complete_run(run_id=run_id, success_count=1, failure_count=0)

        # Get task history
        from rushti.stats import calculate_task_signature

        signature = calculate_task_signature(instance, process, parameters)
        history = self.stats_db.get_task_history(signature, limit=10)

        # Verify history was recorded
        self.assertEqual(len(history), 5)
        # All executions should be successful
        for execution in history:
            self.assertEqual(execution["status"], "Success")

    def test_logging_and_stats_integrated_workflow(self):
        """Test complete workflow: log execution and query stats."""
        # Create execution logger
        logger = ExecutionLogger(workflow="workflow-test")
        run_id = logger.run_id

        # Record run start in stats
        start_time = datetime.now()
        self.stats_db.start_run(run_id=run_id, workflow="workflow-test")

        # Execute and log tasks
        tasks = [
            ("extract", "Extract.Data", 5.0, True, None),
            ("transform", "Transform.Data", 10.0, True, None),
            ("load", "Load.Data", 3.0, False, "Connection lost"),
        ]

        for task_id, process, duration, success, error in tasks:
            task_start = start_time
            task_end = task_start + timedelta(seconds=duration)

            # Log to execution logger
            logger.log_task_execution(
                task_id=task_id,
                instance="tm1srv01",
                process=process,
                parameters={},
                success=success,
                start_time=task_start,
                end_time=task_end,
                retry_count=0,
                error_message=error,
            )

            # Record in stats
            self.stats_db.record_task(
                run_id=run_id,
                task_id=task_id,
                instance="tm1srv01",
                process=process,
                parameters={},
                success=success,
                start_time=task_start,
                end_time=task_end,
                retry_count=0,
                error_message=error,
                workflow="workflow-test",
            )

        # Complete run
        self.stats_db.complete_run(run_id=run_id, success_count=2, failure_count=1)

        # Flush logs
        logger.flush()

        # Query stats and verify
        results = self.stats_db.get_run_results(run_id)

        self.assertEqual(len(results), 3)
        self.assertEqual(sum(1 for r in results if r["status"] == "Success"), 2)
        self.assertEqual(sum(1 for r in results if r["status"] == "Fail"), 1)

        # Verify failed task has error message
        failed_task = next(r for r in results if r["task_id"] == "load")
        self.assertEqual(failed_task["error_message"], "Connection lost")


if __name__ == "__main__":
    unittest.main()
