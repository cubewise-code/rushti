"""
Unit tests for utility functions.
Covers TaskAnalysis, AnalysisReport, analyze_runs, and shared permission helpers.
"""

import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rushti.utils import ensure_shared_file, ensure_shared_dir, makedirs_shared
from rushti.taskfile_ops import (
    TaskAnalysis,
    AnalysisReport,
    analyze_runs,
)
from rushti.stats import StatsDatabase


class TestTaskAnalysis(unittest.TestCase):
    """Tests for TaskAnalysis dataclass"""

    def test_task_analysis_creation(self):
        """Test creating TaskAnalysis"""
        analysis = TaskAnalysis(
            task_id="task-1",
            avg_duration=30.5,
            ewma_duration=28.7,
            run_count=10,
            success_rate=0.95,
        )
        self.assertEqual(analysis.task_id, "task-1")
        self.assertEqual(analysis.avg_duration, 30.5)
        self.assertEqual(analysis.ewma_duration, 28.7)
        self.assertEqual(analysis.run_count, 10)
        self.assertEqual(analysis.success_rate, 0.95)


class TestAnalysisReport(unittest.TestCase):
    """Tests for AnalysisReport dataclass"""

    def test_analysis_report_creation(self):
        """Test creating AnalysisReport"""
        task_analysis = TaskAnalysis(
            task_id="t1",
            avg_duration=10.0,
            ewma_duration=9.5,
            run_count=5,
            success_rate=1.0,
        )
        report = AnalysisReport(
            workflow="test-taskfile",
            analysis_date="2024-01-15T10:00:00",
            run_count=5,
            tasks=[task_analysis],
            recommendations=["No issues found"],
            optimized_order=["t1"],
        )
        self.assertEqual(report.workflow, "test-taskfile")
        self.assertEqual(report.run_count, 5)
        self.assertEqual(len(report.tasks), 1)

    def test_analysis_report_to_dict(self):
        """Test AnalysisReport.to_dict()"""
        task_analysis = TaskAnalysis(
            task_id="t1",
            avg_duration=10.0,
            ewma_duration=9.5,
            run_count=5,
            success_rate=0.8,
        )
        report = AnalysisReport(
            workflow="test-taskfile",
            analysis_date="2024-01-15T10:00:00",
            run_count=5,
            tasks=[task_analysis],
            recommendations=["Optimize slow tasks"],
            optimized_order=["t1"],
        )
        d = report.to_dict()

        self.assertEqual(d["workflow"], "test-taskfile")
        self.assertEqual(len(d["tasks"]), 1)
        self.assertEqual(d["tasks"][0]["task_id"], "t1")
        self.assertEqual(d["tasks"][0]["success_rate"], 0.8)
        self.assertIn("Optimize slow tasks", d["recommendations"])


class TestAnalyzeRuns(unittest.TestCase):
    """Tests for analyze_runs function"""

    def test_analyze_runs_no_data(self):
        """Test analyze_runs with no historical data"""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test_stats.db")
            stats_db = StatsDatabase(db_path=db_path, enabled=True)

            report = analyze_runs(
                workflow="test-taskfile",
                stats_db=stats_db,
                run_count=10,
            )

            self.assertEqual(report.workflow, "test-taskfile")
            self.assertEqual(report.run_count, 0)
            self.assertEqual(report.tasks, [])
            self.assertIn("No historical data available", report.recommendations[0])

            stats_db.close()

    def test_analyze_runs_disabled_stats_db(self):
        """Test analyze_runs with disabled stats database raises error"""
        stats_db = StatsDatabase(enabled=False)

        with self.assertRaises(ValueError) as ctx:
            analyze_runs(
                workflow="test-taskfile",
                stats_db=stats_db,
            )
        self.assertIn("Stats database must be enabled", str(ctx.exception))


class TestSharedPermissions(unittest.TestCase):
    """Tests for shared file/directory permission helpers."""

    @unittest.skipIf(os.name == "nt", "POSIX permissions not applicable on Windows")
    def test_ensure_shared_file_sets_rw_for_all(self):
        """Test that ensure_shared_file sets rw-rw-rw- permissions."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test")
            path = f.name

        try:
            # Start with restrictive permissions
            os.chmod(path, 0o600)
            ensure_shared_file(path)
            mode = os.stat(path).st_mode
            self.assertTrue(mode & stat.S_IRUSR)
            self.assertTrue(mode & stat.S_IWUSR)
            self.assertTrue(mode & stat.S_IRGRP)
            self.assertTrue(mode & stat.S_IWGRP)
            self.assertTrue(mode & stat.S_IROTH)
            self.assertTrue(mode & stat.S_IWOTH)
        finally:
            os.unlink(path)

    @unittest.skipIf(os.name == "nt", "POSIX permissions not applicable on Windows")
    def test_ensure_shared_dir_sets_rwx_for_all(self):
        """Test that ensure_shared_dir sets rwxrwxrwx permissions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = os.path.join(tmpdir, "shared")
            os.makedirs(test_dir)
            os.chmod(test_dir, 0o700)
            ensure_shared_dir(test_dir)
            mode = os.stat(test_dir).st_mode
            self.assertTrue(mode & stat.S_IRWXU)
            self.assertTrue(mode & stat.S_IRWXG)
            self.assertTrue(mode & stat.S_IRWXO)

    @unittest.skipIf(os.name == "nt", "POSIX permissions not applicable on Windows")
    def test_makedirs_shared_creates_with_open_permissions(self):
        """Test that makedirs_shared creates all directories with open permissions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = os.path.join(tmpdir, "a", "b", "c")
            makedirs_shared(nested)

            for d in [
                os.path.join(tmpdir, "a"),
                os.path.join(tmpdir, "a", "b"),
                nested,
            ]:
                self.assertTrue(os.path.isdir(d))
                mode = os.stat(d).st_mode
                self.assertTrue(mode & stat.S_IRWXG)
                self.assertTrue(mode & stat.S_IRWXO)

    def test_makedirs_shared_existing_dir(self):
        """Test that makedirs_shared handles existing directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Should not raise
            makedirs_shared(tmpdir)
            self.assertTrue(os.path.isdir(tmpdir))

    @patch("rushti.utils.os.name", "nt")
    def test_ensure_shared_file_noop_on_windows(self):
        """Test that ensure_shared_file is a no-op on Windows."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name
        try:
            original_mode = os.stat(path).st_mode
            ensure_shared_file(path)
            # On the mock, the function returns early, so mode is unchanged
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
