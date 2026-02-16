"""Unit tests for DAG visualization HTML generation.

Tests for:
- Light theme CSS in generated HTML
- Cross-link support (dashboard_url parameter)
- vis.js configuration
- Logo and header rendering
"""

import os
import tempfile
import unittest

from rushti.taskfile_ops import visualize_dag, _visualize_dag_html
from rushti.taskfile import TaskDefinition


def _make_tasks_by_id():
    """Create a minimal set of TaskDefinition objects for testing."""
    t1 = TaskDefinition(
        id="1",
        process="}bedrock.server.wait",
        instance="tm1srv01",
        predecessors=[],
        parameters={"pWaitSec": "1"},
        stage="extract",
    )
    t2 = TaskDefinition(
        id="2",
        process="}bedrock.server.wait",
        instance="tm1srv01",
        predecessors=["1"],
        parameters={"pWaitSec": "2"},
        stage="load",
    )
    t3 = TaskDefinition(
        id="3",
        process="}bedrock.server.wait",
        instance="tm1srv01",
        predecessors=["1"],
        parameters={},
        stage=None,
    )
    return {"1": t1, "2": t2, "3": t3}


def _make_adjacency(tasks_by_id):
    """Build adjacency list from task definitions."""
    adjacency = {}
    for task_id, task in tasks_by_id.items():
        for pred in task.predecessors:
            if pred not in adjacency:
                adjacency[pred] = []
            adjacency[pred].append(task_id)
    return adjacency


class TestVisualizeDagHtml(unittest.TestCase):
    """Tests for _visualize_dag_html function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.tasks_by_id = _make_tasks_by_id()
        self.adjacency = _make_adjacency(self.tasks_by_id)

    def tearDown(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _generate(self, dashboard_url=None):
        """Helper to generate DAG HTML and return content."""
        filename = os.path.join(self.temp_dir, "test_dag")
        result_path = _visualize_dag_html(
            adjacency=self.adjacency,
            tasks_by_id=self.tasks_by_id,
            filename=filename,
            dashboard_url=dashboard_url,
        )
        with open(result_path, encoding="utf-8") as f:
            return f.read(), result_path

    def test_generates_html_file(self):
        """Test that an HTML file is created."""
        content, path = self._generate()
        self.assertTrue(os.path.isfile(path))
        self.assertTrue(path.endswith(".html"))

    def test_light_theme_css_variables(self):
        """Test that generated HTML uses light theme CSS variables."""
        content, _ = self._generate()
        # Light theme colors
        self.assertIn("--bg-primary: #F8FAFC", content)
        self.assertIn("--bg-secondary: #FFFFFF", content)
        self.assertIn("--text-primary: #1E293B", content)
        self.assertIn("--accent-primary: #00AEEF", content)
        self.assertIn("--border-color: #E2E8F0", content)
        # Should NOT contain dark theme colors
        self.assertNotIn("#0a0a1a", content)
        self.assertNotIn("#12122a", content)

    def test_no_dark_gradients(self):
        """Test that blue-purple gradient is removed from header."""
        content, _ = self._generate()
        self.assertNotIn("linear-gradient(135deg, var(--accent-blue)", content)

    def test_vis_js_included(self):
        """Test that vis.js CDN script is included."""
        content, _ = self._generate()
        self.assertIn("vis-network", content)

    def test_nodes_data_embedded(self):
        """Test that node data is embedded as JSON."""
        content, _ = self._generate()
        self.assertIn("var nodesData =", content)
        self.assertIn("var edgesData =", content)
        # Check all task IDs are present
        self.assertIn('"1"', content)
        self.assertIn('"2"', content)
        self.assertIn('"3"', content)

    def test_edge_colors_for_light_theme(self):
        """Test that vis.js edge colors are suited for light background."""
        content, _ = self._generate()
        self.assertIn("rgba(100, 116, 139, 0.4)", content)  # light-friendly edges
        self.assertIn('"#00AEEF"', content)  # cyan highlight

    def test_node_shadow_lightened(self):
        """Test that node shadows are light (not dark theme)."""
        content, _ = self._generate()
        self.assertIn("rgba(0, 0, 0, 0.1)", content)  # light shadow

    def test_logo_present(self):
        """Test that the RushTI logo SVG is included."""
        content, _ = self._generate()
        self.assertIn("<svg", content)
        self.assertIn("viewBox", content)

    def test_header_title(self):
        """Test that header shows DAG Visualization title."""
        content, _ = self._generate()
        self.assertIn("DAG Visualization", content)
        self.assertIn("Interactive Task Dependency Graph", content)

    def test_dashboard_link_present(self):
        """Test that dashboard link appears when dashboard_url is provided."""
        content, _ = self._generate(dashboard_url="dashboard.html")
        self.assertIn("dashboard.html", content)
        self.assertIn("Performance Dashboard", content)

    def test_dashboard_link_absent_when_none(self):
        """Test that dashboard link is absent when dashboard_url is None."""
        content, _ = self._generate(dashboard_url=None)
        self.assertNotIn("Performance Dashboard", content)

    def test_stage_legend(self):
        """Test that stage legend is rendered for used stages."""
        content, _ = self._generate()
        # extract and load are used in our test data
        self.assertIn('data-stage="extract"', content)
        self.assertIn('data-stage="load"', content)

    def test_nostage_for_unstaged_tasks(self):
        """Test that NoStage is used for tasks without a stage."""
        content, _ = self._generate()
        self.assertIn("NoStage", content)

    def test_view_modes(self):
        """Test that all three view mode buttons are present."""
        content, _ = self._generate()
        self.assertIn('data-view="compact"', content)
        self.assertIn('data-view="detailed"', content)
        self.assertIn('data-view="table"', content)

    def test_search_box(self):
        """Test that search functionality is present."""
        content, _ = self._generate()
        self.assertIn('id="searchBox"', content)

    def test_sidebar_present(self):
        """Test that task details sidebar is present."""
        content, _ = self._generate()
        self.assertIn('id="sidebar"', content)
        self.assertIn("Task Details", content)

    def test_stage_badge_white_text(self):
        """Test that stage badges use white text in light theme."""
        content, _ = self._generate()
        self.assertIn(".stage-badge", content)
        self.assertIn("color: white", content)

    def test_node_highlight_cyan(self):
        """Test that node highlight uses cyan (#00AEEF) for selection."""
        content, _ = self._generate()
        self.assertIn('"#00AEEF"', content)  # highlight color
        self.assertIn('"#0097D4"', content)  # highlight border
        # Old purple highlight for node selection should be gone
        # (note: #8b5cf6 still exists as a stage color for "analysis")


class TestVisualizeDag(unittest.TestCase):
    """Tests for the public visualize_dag function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_from_json_file(self):
        """Test generating DAG from a JSON taskfile."""
        # Use the example taskfile
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        taskfile = os.path.join(
            project_root, "docs", "examples", "Tasks_optimization_inefficient.json"
        )
        if not os.path.isfile(taskfile):
            self.skipTest("Example taskfile not found")

        output = os.path.join(self.temp_dir, "dag.html")
        result = visualize_dag(source=taskfile, output_path=output)
        self.assertTrue(os.path.isfile(result))

        with open(result, encoding="utf-8") as f:
            content = f.read()
        # Verify light theme
        self.assertIn("--bg-primary: #F8FAFC", content)

    def test_dashboard_url_passthrough(self):
        """Test that dashboard_url is passed through to the HTML."""
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        taskfile = os.path.join(
            project_root, "docs", "examples", "Tasks_optimization_inefficient.json"
        )
        if not os.path.isfile(taskfile):
            self.skipTest("Example taskfile not found")

        output = os.path.join(self.temp_dir, "dag.html")
        result = visualize_dag(
            source=taskfile,
            output_path=output,
            dashboard_url="my_dashboard.html",
        )
        with open(result, encoding="utf-8") as f:
            content = f.read()
        self.assertIn("my_dashboard.html", content)
        self.assertIn("Performance Dashboard", content)


if __name__ == "__main__":
    unittest.main()
