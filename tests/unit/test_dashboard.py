"""Unit tests for the dashboard HTML generator module.

Tests for:
- Dashboard HTML generation
- Statistics computation (_compute_run_stats, _prepare_dashboard_data)
- Cross-link support (dag_url parameter)
- Page size options and help tooltips
"""

import os
import tempfile
import unittest
from datetime import datetime, timedelta

from rushti.dashboard import (
    _compute_run_stats,
    _compute_concurrency_timeline,
    _prepare_dashboard_data,
    generate_dashboard,
    _LOGO_SVG,
)


def _make_run(
    run_id="run-001",
    workflow="test_taskfile",
    status="Success",
    task_count=5,
    success_count=5,
    failure_count=0,
    duration=60.0,
    max_workers=2,
    start_time=None,
    taskfile_path="tasks.json",
):
    """Helper to create a mock run dict."""
    if start_time is None:
        start_time = datetime.now().isoformat()
    return {
        "run_id": run_id,
        "workflow": workflow,
        "taskfile_path": taskfile_path,
        "taskfile_name": "Test Taskfile",
        "taskfile_description": "A test",
        "taskfile_author": "Tester",
        "start_time": start_time,
        "end_time": (datetime.fromisoformat(start_time) + timedelta(seconds=duration)).isoformat(),
        "duration_seconds": duration,
        "status": status,
        "task_count": task_count,
        "success_count": success_count,
        "failure_count": failure_count,
        "max_workers": max_workers,
        "retries": 0,
        "result_file": None,
        "exclusive": False,
        "optimize": True,
    }


def _make_task_result(
    run_id="run-001",
    task_id="1",
    process="process1",
    instance="tm1srv01",
    status="Success",
    duration=5.0,
    task_signature="abcdef1234567890",
    stage=None,
):
    """Helper to create a mock task result dict."""
    start = datetime.now()
    return {
        "run_id": run_id,
        "task_id": task_id,
        "task_signature": task_signature,
        "instance": instance,
        "process": process,
        "status": status,
        "duration_seconds": duration,
        "start_time": start.isoformat(),
        "end_time": (start + timedelta(seconds=duration)).isoformat(),
        "error_message": None if status == "Success" else "Task failed",
        "stage": stage,
    }


class TestComputeRunStats(unittest.TestCase):
    """Tests for _compute_run_stats."""

    def test_basic_stats(self):
        """Test basic statistics computation."""
        run = _make_run()
        tasks = [
            _make_task_result(duration=1.0),
            _make_task_result(duration=2.0, task_id="2", task_signature="sig2"),
            _make_task_result(duration=3.0, task_id="3", task_signature="sig3"),
            _make_task_result(duration=4.0, task_id="4", task_signature="sig4"),
            _make_task_result(duration=5.0, task_id="5", task_signature="sig5"),
        ]
        stats = _compute_run_stats(run, tasks)

        self.assertEqual(stats["min"], 1.0)
        self.assertEqual(stats["max"], 5.0)
        self.assertEqual(stats["median"], 3.0)
        self.assertEqual(stats["mean"], 3.0)

    def test_empty_tasks(self):
        """Test stats with no tasks."""
        run = _make_run()
        stats = _compute_run_stats(run, [])
        self.assertEqual(stats["min"], 0)
        self.assertEqual(stats["max"], 0)
        self.assertEqual(stats["median"], 0)

    def test_all_failed_tasks(self):
        """Test stats when all tasks failed (no successful durations)."""
        run = _make_run()
        tasks = [
            _make_task_result(status="Failed", duration=5.0),
            _make_task_result(status="Failed", duration=10.0, task_id="2"),
        ]
        stats = _compute_run_stats(run, tasks)
        self.assertEqual(stats["min"], 0)
        self.assertEqual(stats["max"], 0)

    def test_single_task(self):
        """Test stats with a single task."""
        run = _make_run()
        tasks = [_make_task_result(duration=7.5)]
        stats = _compute_run_stats(run, tasks)
        self.assertEqual(stats["min"], 7.5)
        self.assertEqual(stats["max"], 7.5)
        self.assertEqual(stats["median"], 7.5)
        self.assertEqual(stats["std_dev"], 0)


class TestComputeConcurrencyTimeline(unittest.TestCase):
    """Tests for _compute_concurrency_timeline."""

    def test_empty_tasks(self):
        """Test concurrency with no tasks."""
        run = _make_run()
        timeline = _compute_concurrency_timeline(run, [])
        self.assertEqual(timeline, [])

    def test_single_task(self):
        """Test concurrency with one task."""
        start = datetime.now()
        run = _make_run(start_time=start.isoformat(), duration=10.0)
        tasks = [
            {
                "run_id": "run-001",
                "start_time": (start + timedelta(seconds=1)).isoformat(),
                "end_time": (start + timedelta(seconds=4)).isoformat(),
                "status": "Success",
                "duration_seconds": 3.0,
            }
        ]
        timeline = _compute_concurrency_timeline(run, tasks)
        self.assertGreater(len(timeline), 0)
        # At least one entry should show 1 concurrent task
        max_count = max(t["count"] for t in timeline)
        self.assertGreaterEqual(max_count, 1)


class TestPrepareDashboardData(unittest.TestCase):
    """Tests for _prepare_dashboard_data."""

    def test_basic_data_structure(self):
        """Test that prepared data has expected keys."""
        runs = [_make_run()]
        tasks = [_make_task_result()]
        data = _prepare_dashboard_data(runs, tasks, default_runs=5)

        expected_keys = {
            "workflow",
            "taskfile_name",
            "taskfile_description",
            "taskfile_author",
            "generated_at",
            "default_runs",
            "total_runs",
            "runs",
            "task_results",
            "task_summaries",
            "outliers",
            "failures",
        }
        self.assertTrue(expected_keys.issubset(set(data.keys())))

    def test_default_runs_capped(self):
        """Test that default_runs is capped to total available runs."""
        runs = [_make_run(run_id=f"run-{i}") for i in range(3)]
        tasks = [_make_task_result(run_id=f"run-{i}") for i in range(3)]
        data = _prepare_dashboard_data(runs, tasks, default_runs=10)
        self.assertEqual(data["default_runs"], 3)

    def test_task_results_included(self):
        """Test that slim task_results are embedded in data."""
        runs = [_make_run()]
        tasks = [
            _make_task_result(task_id="1"),
            _make_task_result(task_id="2", task_signature="sig2"),
        ]
        data = _prepare_dashboard_data(runs, tasks, default_runs=5)
        self.assertEqual(len(data["task_results"]), 2)

    def test_stage_field_in_task_results(self):
        """Test that stage field is included in slim task results."""
        runs = [_make_run()]
        tasks = [_make_task_result(stage="extract")]
        data = _prepare_dashboard_data(runs, tasks, default_runs=5)
        self.assertEqual(data["task_results"][0]["stage"], "extract")


class TestGenerateDashboard(unittest.TestCase):
    """Tests for generate_dashboard HTML generation."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.runs = [
            _make_run(start_time=(datetime.now() - timedelta(hours=i)).isoformat())
            for i in range(3)
        ]
        self.tasks = []
        for run in self.runs:
            for j in range(3):
                self.tasks.append(
                    _make_task_result(
                        run_id=run["run_id"],
                        task_id=str(j + 1),
                        task_signature=f"sig{j}",
                        duration=float(j + 1),
                    )
                )

    def tearDown(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_generates_html_file(self):
        """Test that an HTML file is created."""
        output = os.path.join(self.temp_dir, "test_dashboard.html")
        result = generate_dashboard("test_id", self.runs, self.tasks, output)
        self.assertTrue(os.path.isfile(result))
        self.assertTrue(result.endswith(".html"))

    def test_html_contains_chart_js(self):
        """Test that generated HTML includes Chart.js CDN."""
        output = os.path.join(self.temp_dir, "test_dashboard.html")
        generate_dashboard("test_id", self.runs, self.tasks, output)
        with open(output, encoding="utf-8") as f:
            content = f.read()
        self.assertIn("chart.js", content.lower())

    def test_html_contains_logo(self):
        """Test that generated HTML includes the RushTI logo SVG."""
        output = os.path.join(self.temp_dir, "test_dashboard.html")
        generate_dashboard("test_id", self.runs, self.tasks, output)
        with open(output, encoding="utf-8") as f:
            content = f.read()
        self.assertIn("svg", content)

    def test_html_contains_embedded_data(self):
        """Test that all data is embedded as JSON."""
        output = os.path.join(self.temp_dir, "test_dashboard.html")
        generate_dashboard("test_id", self.runs, self.tasks, output)
        with open(output, encoding="utf-8") as f:
            content = f.read()
        self.assertIn("const DATA =", content)
        self.assertIn("test_id", content)

    def test_help_tooltips_present(self):
        """Test that help tooltips with 'How to read:' are included."""
        output = os.path.join(self.temp_dir, "test_dashboard.html")
        generate_dashboard("test_id", self.runs, self.tasks, output)
        with open(output, encoding="utf-8") as f:
            content = f.read()
        self.assertIn("How to read:", content)

    def test_page_size_options(self):
        """Test that per-task table has correct page size options (10, 25, 50, All)."""
        output = os.path.join(self.temp_dir, "test_dashboard.html")
        generate_dashboard("test_id", self.runs, self.tasks, output)
        with open(output, encoding="utf-8") as f:
            content = f.read()
        self.assertIn('<option value="10">10</option>', content)
        self.assertIn('<option value="25">25</option>', content)
        self.assertIn('<option value="50">50</option>', content)
        self.assertIn('<option value="0">All</option>', content)
        # Should NOT contain old values
        self.assertNotIn('<option value="100">100</option>', content)
        self.assertNotIn('<option value="200">200</option>', content)

    def test_dag_url_link_present(self):
        """Test that 'View DAG' link appears when dag_url is provided."""
        output = os.path.join(self.temp_dir, "test_dashboard.html")
        generate_dashboard("test_id", self.runs, self.tasks, output, dag_url="test_dag.html")
        with open(output, encoding="utf-8") as f:
            content = f.read()
        self.assertIn("test_dag.html", content)
        self.assertIn("View DAG", content)

    def test_dag_url_link_absent_when_none(self):
        """Test that 'View DAG' link is absent when dag_url is None."""
        output = os.path.join(self.temp_dir, "test_dashboard.html")
        generate_dashboard("test_id", self.runs, self.tasks, output)
        with open(output, encoding="utf-8") as f:
            content = f.read()
        self.assertNotIn("View DAG", content)

    def test_creates_parent_directories(self):
        """Test that output parent directories are created."""
        output = os.path.join(self.temp_dir, "subdir", "deep", "dashboard.html")
        result = generate_dashboard("test_id", self.runs, self.tasks, output)
        self.assertTrue(os.path.isfile(result))

    def test_light_theme_colors(self):
        """Test that dashboard uses light theme colors."""
        output = os.path.join(self.temp_dir, "test_dashboard.html")
        generate_dashboard("test_id", self.runs, self.tasks, output)
        with open(output, encoding="utf-8") as f:
            content = f.read()
        # Light theme indicators
        self.assertIn("#F8FAFC", content)  # light bg
        self.assertIn("#1E293B", content)  # dark text
        self.assertIn("#00AEEF", content)  # accent cyan


class TestLogoSVG(unittest.TestCase):
    """Tests for the _LOGO_SVG constant."""

    def test_logo_is_svg(self):
        """Test that logo is a valid SVG string."""
        self.assertIn("<svg", _LOGO_SVG)
        self.assertIn("</svg>", _LOGO_SVG)

    def test_logo_has_viewbox(self):
        """Test that logo has a viewBox for proper scaling."""
        self.assertIn("viewBox", _LOGO_SVG)


if __name__ == "__main__":
    unittest.main()
