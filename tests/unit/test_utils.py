"""
Unit tests for utility functions.
Covers TaskAnalysis, AnalysisReport, and analyze_runs function.
"""

import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
