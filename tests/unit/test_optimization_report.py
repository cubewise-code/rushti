"""Unit tests for the optimization report HTML generator module.

Tests for:
- Report data preparation (_prepare_report_data)
- HTML report generation (generate_optimization_report)
- Chart.js and embedded data presence
- Help tooltips and educational links
- Chain diagram rendering
- Worker formula breakdown
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

from rushti.contention_analyzer import (
    ContentionAnalysisResult,
    ContentionGroup,
    ParameterAnalysis,
)
from rushti.optimization_report import (
    _compute_chain_sequences,
    _format_duration,
    _prepare_report_data,
    generate_optimization_report,
)


def _make_result(
    contention_driver="pSegment",
    heavy_count=2,
    light_count=8,
    fan_out_size=3,
    sensitivity=10.0,
):
    """Create a representative ContentionAnalysisResult for testing."""
    heavy_groups = [
        ContentionGroup(
            driver_value=f"H{i}",
            task_ids=[f"t_H{i}_{j}" for j in range(fan_out_size)],
            avg_duration=200.0 - i * 20,
            is_heavy=True,
        )
        for i in range(heavy_count)
    ]
    light_groups = [
        ContentionGroup(
            driver_value=f"L{i}",
            task_ids=[f"t_L{i}_{j}" for j in range(fan_out_size)],
            avg_duration=10.0 + i,
            is_heavy=False,
        )
        for i in range(light_count)
    ]

    all_groups = heavy_groups + light_groups

    # Build predecessor map (chain heavy groups for each fan-out value)
    predecessor_map = {}
    if heavy_count >= 2:
        for j in range(fan_out_size):
            for i in range(1, heavy_count):
                predecessor_map[f"t_H{i}_{j}"] = [f"t_H{i - 1}_{j}"]

    analyses = [
        ParameterAnalysis(
            key="pSegment",
            distinct_values=heavy_count + light_count,
            group_averages={g.driver_value: g.avg_duration for g in all_groups},
            range_seconds=190.0,
        ),
        ParameterAnalysis(
            key="pPeriod",
            distinct_values=fan_out_size,
            group_averages={str(i): 50.0 + i for i in range(fan_out_size)},
            range_seconds=2.0,
        ),
    ]

    critical_path = sum(g.avg_duration for g in heavy_groups)

    return ContentionAnalysisResult(
        contention_driver=contention_driver,
        fan_out_keys=["pPeriod"],
        heavy_groups=heavy_groups,
        light_groups=light_groups,
        all_groups=all_groups,
        chain_length=heavy_count,
        fan_out_size=fan_out_size,
        critical_path_seconds=critical_path,
        recommended_workers=fan_out_size + 2,
        sensitivity=sensitivity,
        iqr_stats={"q1": 12.0, "q3": 18.0, "iqr": 6.0, "upper_fence": 78.0},
        predecessor_map=predecessor_map,
        parameter_analyses=analyses,
    )


class TestFormatDuration(unittest.TestCase):
    """Tests for _format_duration helper."""

    def test_seconds_only(self):
        self.assertEqual(_format_duration(45.2), "45.2s")

    def test_minutes_and_seconds(self):
        self.assertEqual(_format_duration(125.0), "2m 5s")

    def test_zero(self):
        self.assertEqual(_format_duration(0.0), "0.0s")

    def test_large_value(self):
        result = _format_duration(3661.5)
        self.assertIn("m", result)


class TestPrepareReportData(unittest.TestCase):
    """Tests for _prepare_report_data."""

    def setUp(self):
        self.result = _make_result()
        self.data = _prepare_report_data("test_workflow", self.result)

    def test_basic_keys_present(self):
        expected_keys = [
            "workflow",
            "generated_at",
            "contention_driver",
            "fan_out_keys",
            "fan_out_size",
            "total_tasks",
            "heavy_task_count",
            "light_task_count",
            "sensitivity",
            "chain_length",
            "critical_path_seconds",
            "recommended_workers",
            "iqr_stats",
            "warnings",
            "all_groups",
            "heavy_groups",
            "light_groups",
            "parameter_analyses",
            "chains",
            "worker_breakdown",
            "predecessor_count",
        ]
        for key in expected_keys:
            self.assertIn(key, self.data, f"Missing key: {key}")

    def test_workflow_name(self):
        self.assertEqual(self.data["workflow"], "test_workflow")

    def test_groups_serialized(self):
        self.assertEqual(len(self.data["all_groups"]), 10)
        self.assertEqual(len(self.data["heavy_groups"]), 2)
        self.assertEqual(len(self.data["light_groups"]), 8)

        # Check group structure
        group = self.data["all_groups"][0]
        self.assertIn("driver_value", group)
        self.assertIn("task_count", group)
        self.assertIn("avg_duration", group)
        self.assertIn("is_heavy", group)

    def test_parameter_analyses_serialized(self):
        self.assertEqual(len(self.data["parameter_analyses"]), 2)

        winner = next(a for a in self.data["parameter_analyses"] if a["is_winner"])
        self.assertEqual(winner["key"], "pSegment")
        self.assertGreater(winner["range_seconds"], 0)

        non_winner = next(a for a in self.data["parameter_analyses"] if not a["is_winner"])
        self.assertEqual(non_winner["key"], "pPeriod")

    def test_chains_computed(self):
        chains = self.data["chains"]
        self.assertEqual(len(chains), 3)  # fan_out_size=3
        for chain in chains:
            self.assertIn("fan_out_value", chain)
            self.assertIn("sequence", chain)
            self.assertEqual(len(chain["sequence"]), 2)  # 2 heavy groups
            self.assertEqual(chain["sequence"][0], "H0")  # heaviest first
            self.assertEqual(chain["sequence"][1], "H1")

    def test_worker_breakdown_computed(self):
        wb = self.data["worker_breakdown"]
        self.assertEqual(wb["chain_slots"], 3)  # fan_out_size
        self.assertGreater(wb["light_total_work"], 0)
        self.assertGreater(wb["critical_path"], 0)
        self.assertEqual(wb["total"], 5)  # fan_out_size + 2

    def test_iqr_stats_present(self):
        iqr = self.data["iqr_stats"]
        self.assertEqual(iqr["q1"], 12.0)
        self.assertEqual(iqr["q3"], 18.0)
        self.assertEqual(iqr["iqr"], 6.0)
        self.assertEqual(iqr["upper_fence"], 78.0)

    def test_json_serializable(self):
        """All data must be JSON-serializable."""
        serialized = json.dumps(self.data, default=str)
        self.assertIsInstance(serialized, str)
        parsed = json.loads(serialized)
        self.assertEqual(parsed["workflow"], "test_workflow")


class TestPrepareReportDataEmpty(unittest.TestCase):
    """Tests for _prepare_report_data with no contention driver."""

    def test_empty_result_handled(self):
        result = ContentionAnalysisResult(
            contention_driver=None,
            fan_out_keys=[],
            heavy_groups=[],
            light_groups=[],
            all_groups=[],
            chain_length=0,
            fan_out_size=0,
            critical_path_seconds=0.0,
            recommended_workers=0,
            sensitivity=10.0,
            iqr_stats={},
            predecessor_map={},
            warnings=["No historical data found"],
        )
        data = _prepare_report_data("empty_workflow", result)
        self.assertEqual(data["contention_driver"], "none")
        self.assertEqual(len(data["all_groups"]), 0)
        self.assertEqual(len(data["chains"]), 0)
        self.assertEqual(data["warnings"], ["No historical data found"])


class TestComputeChainSequences(unittest.TestCase):
    """Tests for _compute_chain_sequences."""

    def test_chains_with_heavy_groups(self):
        result = _make_result(heavy_count=3, fan_out_size=4)
        chains = _compute_chain_sequences(result)
        self.assertEqual(len(chains), 4)
        for chain in chains:
            self.assertEqual(len(chain["sequence"]), 3)

    def test_no_chains_when_fewer_than_2_heavy(self):
        result = _make_result(heavy_count=1, fan_out_size=3)
        # Clear predecessor map since it's empty with 1 heavy group
        result.predecessor_map = {}
        chains = _compute_chain_sequences(result)
        self.assertEqual(len(chains), 0)

    def test_no_chains_when_no_predecessors(self):
        result = _make_result(heavy_count=3, fan_out_size=2)
        result.predecessor_map = {}
        chains = _compute_chain_sequences(result)
        self.assertEqual(len(chains), 0)


class TestGenerateOptimizationReport(unittest.TestCase):
    """Tests for generate_optimization_report."""

    def setUp(self):
        self.result = _make_result()
        self.tmpdir = tempfile.mkdtemp()
        self.output_path = os.path.join(self.tmpdir, "test_report.html")

    def tearDown(self):
        if os.path.exists(self.output_path):
            os.remove(self.output_path)
        if os.path.exists(self.tmpdir):
            os.rmdir(self.tmpdir)

    def _generate(self):
        return generate_optimization_report(
            workflow="test_workflow",
            result=self.result,
            output_path=self.output_path,
            open_browser=False,
        )

    def test_generates_html_file(self):
        path = self._generate()
        self.assertTrue(os.path.exists(path))
        content = Path(path).read_text(encoding="utf-8")
        self.assertTrue(content.startswith("<!DOCTYPE html>"))

    def test_returns_output_path(self):
        path = self._generate()
        self.assertEqual(path, self.output_path)

    def test_html_contains_chart_js(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("cdn.jsdelivr.net/npm/chart.js@4", content)

    def test_html_contains_logo(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("<svg", content)
        self.assertIn("viewBox", content)

    def test_html_contains_embedded_data(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("const DATA =", content)

    def test_html_contains_help_tooltips(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("help-icon", content)
        self.assertIn("help-tip", content)

    def test_html_contains_wikipedia_links(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("en.wikipedia.org/wiki/Interquartile_range", content)
        self.assertIn("en.wikipedia.org/wiki/Exponential_smoothing", content)
        self.assertIn("en.wikipedia.org/wiki/Directed_acyclic_graph", content)
        self.assertIn("en.wikipedia.org/wiki/Box_plot", content)
        self.assertIn("en.wikipedia.org/wiki/Outlier", content)

    def test_html_contains_contention_driver(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("pSegment", content)

    def test_html_contains_iqr_stats(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("78.0s", content)  # upper_fence
        self.assertIn("Q1", content)
        self.assertIn("Q3", content)

    def test_html_contains_workflow_name(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("test_workflow", content)

    def test_html_contains_chain_section(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("chainContainer", content)
        self.assertIn("Predecessor Chain Structure", content)

    def test_html_contains_parameter_variance_chart(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("paramVarianceChart", content)
        self.assertIn("Parameter Variance Analysis", content)

    def test_html_contains_group_distribution_chart(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("groupDistChart", content)
        self.assertIn("Group Duration Distribution", content)

    def test_html_contains_formula(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("formulaPanel", content)
        self.assertIn("Worker Recommendation", content)

    def test_html_contains_group_table(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("groupTableBody", content)

    def test_html_contains_footer(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("Generated by", content)
        self.assertIn("cubewise-code/rushti", content)

    def test_creates_parent_directories(self):
        nested_path = os.path.join(self.tmpdir, "sub", "dir", "report.html")
        path = generate_optimization_report(
            workflow="test",
            result=self.result,
            output_path=nested_path,
            open_browser=False,
        )
        self.assertTrue(os.path.exists(path))
        # Clean up
        os.remove(nested_path)
        os.rmdir(os.path.join(self.tmpdir, "sub", "dir"))
        os.rmdir(os.path.join(self.tmpdir, "sub"))

    def test_light_theme_colors(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("#F8FAFC", content)  # page bg
        self.assertIn("#00AEEF", content)  # accent blue
        self.assertIn("#1E293B", content)  # primary text
        self.assertIn("#E2E8F0", content)  # border

    def test_upper_fence_plugin(self):
        self._generate()
        content = Path(self.output_path).read_text(encoding="utf-8")
        self.assertIn("upperFenceLine", content)
        self.assertIn("Upper Fence", content)


class TestGenerateReportWithWarnings(unittest.TestCase):
    """Tests for report generation with warnings."""

    def test_warnings_section_rendered(self):
        result = _make_result()
        result.warnings = ["Only 1 heavy group detected", "Consider lowering sensitivity"]

        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            output_path = f.name

        try:
            generate_optimization_report(
                workflow="test",
                result=result,
                output_path=output_path,
                open_browser=False,
            )
            content = Path(output_path).read_text(encoding="utf-8")
            self.assertIn("Warnings", content)
            self.assertIn("Only 1 heavy group detected", content)
            self.assertIn("Consider lowering sensitivity", content)
        finally:
            os.remove(output_path)


class TestGenerateReportNoOptimization(unittest.TestCase):
    """Tests for report when no optimization was applied."""

    def test_no_chains_report(self):
        result = _make_result(heavy_count=0, light_count=5)
        result.predecessor_map = {}
        result.chain_length = 0

        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            output_path = f.name

        try:
            generate_optimization_report(
                workflow="test",
                result=result,
                output_path=output_path,
                open_browser=False,
            )
            content = Path(output_path).read_text(encoding="utf-8")
            # Should still render (diagnostic info)
            self.assertIn("Contention Analysis Report", content)
            self.assertIn("const DATA =", content)
        finally:
            os.remove(output_path)


if __name__ == "__main__":
    unittest.main()
