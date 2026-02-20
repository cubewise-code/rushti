"""Unit tests for database administration utilities.

Tests the db_admin module which provides SQLite database management
for the RushTI stats database including viewing statistics, listing data,
clearing records, and maintenance operations.
"""

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta

from rushti.db_admin import (
    clear_all,
    clear_before_date,
    clear_run,
    clear_workflow,
    export_to_csv,
    get_db_stats,
    get_workflow_stats,
    list_runs,
    list_workflows,
    list_tasks,
    show_run_details,
    show_task_history,
    vacuum_database,
)


class TestDatabaseAdminUtilities(unittest.TestCase):
    """Tests for database admin utility functions."""

    def setUp(self):
        """Create a temporary database with sample data for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".db")
        self.temp_db.close()
        self.db_path = self.temp_db.name

        # Create database schema
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create task_results table
        cursor.execute("""
            CREATE TABLE task_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                workflow TEXT NOT NULL,
                task_id TEXT NOT NULL,
                task_signature TEXT NOT NULL,
                instance TEXT NOT NULL,
                process TEXT NOT NULL,
                parameters TEXT,
                status TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                duration_seconds REAL,
                retry_count INTEGER DEFAULT 0,
                error_message TEXT
            )
        """)

        # Create runs table
        cursor.execute("""
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                workflow TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT,
                duration_seconds REAL,
                task_count INTEGER,
                success_count INTEGER,
                failure_count INTEGER
            )
        """)

        # Insert sample data
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)

        # Sample task results
        sample_data = [
            (
                "run1",
                "taskfile1",
                "task1",
                "sig1",
                "tm1srv01",
                "proc1",
                "{}",
                "Success",
                week_ago.isoformat(),
                week_ago.isoformat(),
                1.5,
                0,
                None,
            ),
            (
                "run1",
                "taskfile1",
                "task2",
                "sig2",
                "tm1srv01",
                "proc2",
                "{}",
                "Success",
                week_ago.isoformat(),
                week_ago.isoformat(),
                2.0,
                0,
                None,
            ),
            (
                "run2",
                "taskfile1",
                "task1",
                "sig1",
                "tm1srv01",
                "proc1",
                "{}",
                "Fail",
                yesterday.isoformat(),
                yesterday.isoformat(),
                1.0,
                1,
                "Error message",
            ),
            (
                "run3",
                "taskfile2",
                "task1",
                "sig3",
                "tm1srv02",
                "proc3",
                "{}",
                "Success",
                now.isoformat(),
                now.isoformat(),
                3.0,
                0,
                None,
            ),
        ]

        cursor.executemany(
            """
            INSERT INTO task_results
            (run_id, workflow, task_id, task_signature, instance, process, parameters,
             status, start_time, end_time, duration_seconds, retry_count, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            sample_data,
        )

        # Sample runs
        run_data = [
            ("run1", "taskfile1", week_ago.isoformat(), week_ago.isoformat(), 3.5, 2, 2, 0),
            ("run2", "taskfile1", yesterday.isoformat(), yesterday.isoformat(), 1.0, 1, 0, 1),
            ("run3", "taskfile2", now.isoformat(), now.isoformat(), 3.0, 1, 1, 0),
        ]

        cursor.executemany(
            """
            INSERT INTO runs
            (run_id, workflow, start_time, end_time, duration_seconds,
             task_count, success_count, failure_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            run_data,
        )

        conn.commit()
        conn.close()

    def tearDown(self):
        """Remove temporary database."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_get_db_stats(self):
        """Test retrieving overall database statistics."""
        stats = get_db_stats(self.db_path)

        self.assertTrue(stats["exists"])
        self.assertEqual(stats["total_records"], 4)
        self.assertEqual(stats["workflow_count"], 2)
        self.assertEqual(stats["run_count"], 3)
        self.assertEqual(stats["unique_tasks"], 3)
        self.assertEqual(stats["success_rate"], 75.0)  # 3 success, 1 fail

    def test_get_db_stats_nonexistent(self):
        """Test stats for nonexistent database."""
        stats = get_db_stats("/nonexistent/db.db")

        self.assertFalse(stats["exists"])
        self.assertIn("message", stats)

    def test_get_workflow_stats(self):
        """Test retrieving statistics for a specific workflow."""
        stats = get_workflow_stats("taskfile1", self.db_path)

        self.assertTrue(stats["exists"])
        self.assertEqual(stats["total_records"], 3)
        self.assertEqual(stats["run_count"], 2)
        self.assertEqual(stats["unique_tasks"], 2)

    def test_get_workflow_stats_nonexistent(self):
        """Test stats for nonexistent workflow."""
        stats = get_workflow_stats("nonexistent", self.db_path)

        self.assertFalse(stats["exists"])
        self.assertIn("message", stats)

    def test_list_workflows(self):
        """Test listing all workflows."""
        workflows = list_workflows(self.db_path)

        self.assertEqual(len(workflows), 2)
        workflow_ids = [w["workflow"] for w in workflows]
        self.assertIn("taskfile1", workflow_ids)
        self.assertIn("taskfile2", workflow_ids)

    def test_list_runs(self):
        """Test listing runs for a workflow."""
        runs = list_runs("taskfile1", self.db_path)

        self.assertEqual(len(runs), 2)
        self.assertEqual(runs[0]["task_count"], 1)  # Most recent first
        self.assertEqual(runs[1]["task_count"], 2)

    def test_list_tasks(self):
        """Test listing unique tasks for a workflow."""
        tasks = list_tasks("taskfile1", self.db_path)

        self.assertEqual(len(tasks), 2)
        task_ids = [t["task_id"] for t in tasks]
        self.assertIn("task1", task_ids)
        self.assertIn("task2", task_ids)

    def test_clear_workflow_dry_run(self):
        """Test dry run of clearing workflow data."""
        count = clear_workflow("taskfile1", self.db_path, dry_run=True)

        self.assertEqual(count, 3)  # Would delete 3 records

        # Verify nothing was deleted
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 4)

    def test_clear_workflow(self):
        """Test clearing all data for a workflow."""
        count = clear_workflow("taskfile1", self.db_path, dry_run=False)

        self.assertEqual(count, 3)  # Deleted 3 records

        # Verify deletion
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 1)  # Only taskfile2 remains

    def test_clear_run(self):
        """Test clearing all data for a run."""
        count = clear_run("run1", self.db_path, dry_run=False)

        self.assertEqual(count, 2)  # Deleted 2 records

        # Verify deletion
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 2)

    def test_clear_before_date(self):
        """Test clearing data before a specific date."""
        # Clear data older than 2 days
        cutoff = (datetime.now() - timedelta(days=2)).isoformat()
        count = clear_before_date(cutoff, self.db_path, dry_run=False)

        self.assertGreaterEqual(count, 2)  # Should clear week-old data

    def test_clear_all(self):
        """Test clearing all database data."""
        count = clear_all(self.db_path, dry_run=False)

        self.assertEqual(count, 4)  # All records deleted

        # Verify deletion
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 0)

    def test_vacuum_database(self):
        """Test database vacuum operation."""
        size_before, size_after = vacuum_database(self.db_path)

        self.assertGreaterEqual(size_before, 0)
        self.assertGreaterEqual(size_after, 0)
        # After vacuum, size should be <= original size
        self.assertLessEqual(size_after, size_before)

    def test_export_to_csv(self):
        """Test exporting task results to CSV."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            csv_path = f.name

        try:
            count = export_to_csv(csv_path, db_path=self.db_path)

            self.assertEqual(count, 4)  # All records exported
            self.assertTrue(os.path.exists(csv_path))

            # Verify CSV content
            with open(csv_path, "r") as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 5)  # Header + 4 records

        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)

    def test_export_to_csv_with_filters(self):
        """Test exporting with workflow filter."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            csv_path = f.name

        try:
            count = export_to_csv(csv_path, workflow="taskfile1", db_path=self.db_path)

            self.assertEqual(count, 3)  # Only taskfile1 records

        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)

    def test_show_run_details(self):
        """Test showing detailed run information."""
        details = show_run_details("run1", self.db_path)

        self.assertTrue(details["exists"])
        self.assertEqual(details["run_id"], "run1")
        self.assertEqual(details["workflow"], "taskfile1")
        self.assertEqual(details["task_count"], 2)
        self.assertEqual(details["success_count"], 2)
        self.assertEqual(len(details["tasks"]), 2)

    def test_show_run_details_nonexistent(self):
        """Test showing details for nonexistent run."""
        details = show_run_details("nonexistent", self.db_path)

        self.assertFalse(details["exists"])
        self.assertIn("message", details)

    def test_show_task_history(self):
        """Test showing task execution history."""
        history = show_task_history("sig1", self.db_path)

        self.assertTrue(history["exists"])
        self.assertEqual(history["task_signature"], "sig1")
        self.assertEqual(history["task_id"], "task1")
        self.assertEqual(history["execution_count"], 2)  # Ran twice
        self.assertEqual(len(history["executions"]), 2)

    def test_show_task_history_nonexistent(self):
        """Test showing history for nonexistent task."""
        history = show_task_history("nonexistent", self.db_path)

        self.assertFalse(history["exists"])
        self.assertIn("message", history)


if __name__ == "__main__":
    unittest.main()
