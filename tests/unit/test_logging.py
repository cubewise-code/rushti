"""
Unit tests for execution logging functionality.
Covers TaskExecutionLog, ExecutionRun, ExecutionLogger, and log destinations.
"""

import json
import unittest
from datetime import datetime

from rushti.logging import (
    TaskExecutionLog,
    ExecutionRun,
    ExecutionLogger,
    FileLogDestination,
    create_execution_logger,
)


class TestTaskExecutionLog(unittest.TestCase):
    """Tests for TaskExecutionLog structured log entry"""

    def test_log_entry_creation(self):
        """Test creating a TaskExecutionLog entry"""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 0, 30)

        log = TaskExecutionLog(
            workflow="daily-etl",
            task_id="task1",
            instance="tm1srv01",
            process="test.process",
            parameters='{"pParam1": "value1"}',
            status="Success",
            start_time=start,
            end_time=end,
            duration_seconds=30.0,
            retry_count=0,
            error_message=None,
        )

        self.assertEqual(log.workflow, "daily-etl")
        self.assertEqual(log.task_id, "task1")
        self.assertEqual(log.status, "Success")
        self.assertEqual(log.duration_seconds, 30.0)

    def test_log_entry_to_dict(self):
        """Test converting log entry to dictionary"""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 0, 30)

        log = TaskExecutionLog(
            workflow="daily-etl",
            task_id="task1",
            instance="tm1srv01",
            process="test.process",
            parameters="{}",
            status="Fail",
            start_time=start,
            end_time=end,
            duration_seconds=30.0,
            retry_count=2,
            error_message="Process error",
        )

        d = log.to_dict()
        self.assertEqual(d["status"], "Fail")
        self.assertEqual(d["retry_count"], 2)
        self.assertEqual(d["error_message"], "Process error")
        # Dates should be ISO strings
        self.assertIn("2024-01-15", d["start_time"])

    def test_log_entry_to_json(self):
        """Test converting log entry to JSON string"""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 0, 30)

        log = TaskExecutionLog(
            workflow="daily-etl",
            task_id="task1",
            instance="tm1srv01",
            process="test.process",
            parameters="{}",
            status="Success",
            start_time=start,
            end_time=end,
            duration_seconds=30.0,
        )

        json_str = log.to_json()
        parsed = json.loads(json_str)
        self.assertEqual(parsed["status"], "Success")

    def test_log_from_execution_result(self):
        """Test creating log from execution result"""
        start = datetime.now()
        end = datetime.now()

        log = TaskExecutionLog.from_execution_result(
            workflow="task-file",
            task_id="t1",
            instance="tm1",
            process="proc",
            parameters={"pValue": "123"},
            success=True,
            start_time=start,
            end_time=end,
        )

        self.assertEqual(log.status, "Success")
        self.assertIn("pValue", log.parameters)

    def test_log_from_failed_execution(self):
        """Test creating log from failed execution"""
        start = datetime.now()
        end = datetime.now()

        log = TaskExecutionLog.from_execution_result(
            workflow="task-file",
            task_id="t1",
            instance="tm1",
            process="proc",
            parameters={},
            success=False,
            start_time=start,
            end_time=end,
            error_message="Connection failed",
        )

        self.assertEqual(log.status, "Fail")
        self.assertEqual(log.error_message, "Connection failed")


class TestExecutionRun(unittest.TestCase):
    """Tests for ExecutionRun container"""

    def test_add_log_to_run(self):
        """Test adding logs to an execution run"""
        run = ExecutionRun(
            run_id="20240115_100000",
            workflow="daily-etl",
            start_time=datetime.now(),
        )

        log = TaskExecutionLog(
            workflow="daily-etl",
            task_id="task1",
            instance="tm1srv01",
            process="test.process",
            parameters="{}",
            status="Success",
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=30.0,
        )

        run.add_log(log)
        self.assertEqual(len(run.task_logs), 1)

    def test_run_success_failure_counts(self):
        """Test counting success/failure in run"""
        run = ExecutionRun(
            run_id="20240115_100000",
            workflow="daily-etl",
            start_time=datetime.now(),
        )

        # Add successful log
        run.add_log(
            TaskExecutionLog(
                workflow="daily-etl",
                task_id="t1",
                instance="tm1",
                process="p1",
                parameters="{}",
                status="Success",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_seconds=10.0,
            )
        )

        # Add failed log
        run.add_log(
            TaskExecutionLog(
                workflow="daily-etl",
                task_id="t2",
                instance="tm1",
                process="p2",
                parameters="{}",
                status="Fail",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_seconds=5.0,
            )
        )

        self.assertEqual(run.success_count, 1)
        self.assertEqual(run.failure_count, 1)
        self.assertEqual(run.cumulative_duration_seconds, 15.0)

    def test_wall_clock_seconds(self):
        """Test wall-clock duration uses start/end time, not task sum"""
        from datetime import timedelta

        start = datetime(2026, 1, 15, 10, 0, 0)
        run = ExecutionRun(
            run_id="20260115_100000",
            workflow="daily-etl",
            start_time=start,
        )

        # Add two tasks that each took 100s (simulating parallel execution)
        for task_id in ("t1", "t2"):
            run.add_log(
                TaskExecutionLog(
                    workflow="daily-etl",
                    task_id=task_id,
                    instance="tm1",
                    process="p1",
                    parameters="{}",
                    status="Success",
                    start_time=start,
                    end_time=start + timedelta(seconds=100),
                    duration_seconds=100.0,
                )
            )

        # Complete the run after 120s wall-clock
        run.complete(end_time=start + timedelta(seconds=120))

        # Wall-clock should be 120s (actual elapsed)
        self.assertEqual(run.wall_clock_seconds, 120.0)
        # Cumulative should be 200s (sum of all task durations)
        self.assertEqual(run.cumulative_duration_seconds, 200.0)

    def test_wall_clock_seconds_before_complete(self):
        """Test wall-clock returns 0 when run not yet completed"""
        run = ExecutionRun(
            run_id="20260115_100000",
            workflow="daily-etl",
            start_time=datetime.now(),
        )
        self.assertEqual(run.wall_clock_seconds, 0.0)


class TestExecutionLogger(unittest.TestCase):
    """Tests for ExecutionLogger class"""

    def test_create_logger_default(self):
        """Test creating logger with default file destination"""
        logger = ExecutionLogger(workflow="test-task")
        self.assertEqual(logger.workflow, "test-task")
        self.assertEqual(logger.log_count, 0)
        self.assertIsNotNone(logger.run_id)

    def test_log_task_execution(self):
        """Test logging a task execution"""
        logger = ExecutionLogger(workflow="test-task")

        log = logger.log_task_execution(
            task_id="t1",
            instance="tm1",
            process="test.proc",
            parameters={"pA": "1"},
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        self.assertEqual(logger.log_count, 1)
        self.assertEqual(log.task_id, "t1")
        self.assertEqual(log.status, "Success")


class TestCreateExecutionLogger(unittest.TestCase):
    """Tests for create_execution_logger factory function"""

    def test_create_file_logger(self):
        """Test creating logger with file destination"""
        logger = create_execution_logger(
            workflow="test-task",
        )
        self.assertIsNotNone(logger)
        self.assertEqual(len(logger.destinations), 1)
        self.assertIsInstance(logger.destinations[0], FileLogDestination)


if __name__ == "__main__":
    unittest.main()
