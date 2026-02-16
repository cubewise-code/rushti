"""Unit tests for TM1 build module.

Tests the tm1_build module which creates TM1 objects required for RushTI
logging including dimensions, cubes, views, and sample data.
"""

import unittest
from unittest.mock import Mock, patch

from rushti.tm1_objects import (
    MEASURE_ELEMENTS,
    MEASURE_ATTRIBUTES,
    TASK_ID_ELEMENT_COUNT,
    WORKFLOW_SEED_ELEMENTS,
    RUN_ID_SEED_ELEMENTS,
    SAMPLE_DATA,
)
from rushti.tm1_build import (
    build_logging_objects,
    get_build_status,
    verify_logging_objects,
    _create_workflow_dimension,
    _create_task_id_dimension,
    _create_run_id_dimension,
    _create_measure_dimension,
)

# Default dimension and cube names for testing
DIM_TASKFILE = "rushti_workflow"
DIM_TASK = "rushti_task_id"
DIM_RUN = "rushti_run_id"
DIM_MEASURE = "rushti_measure"
CUBE_LOGS = "rushti"


class TestTM1ObjectsConstants(unittest.TestCase):
    """Tests for tm1_objects module constants."""

    def test_measure_elements_complete(self):
        """Test that all required measures are defined."""
        required_measures = [
            "instance",
            "process",
            "parameters",
            "status",
            "start_time",
            "end_time",
            "duration_seconds",
            "retries",
            "retry_count",
            "error_message",
            "predecessors",
            "stage",
            "safe_retry",
            "timeout",
            "cancel_at_timeout",
            "require_predecessor_success",
            "succeed_on_minor_errors",
            "wait",
        ]
        for measure in required_measures:
            self.assertIn(measure, MEASURE_ELEMENTS)

    def test_measure_attributes_match_elements(self):
        """Test that every measure element has an attribute entry."""
        for elem in MEASURE_ELEMENTS:
            self.assertIn(elem, MEASURE_ATTRIBUTES)

    def test_measure_attributes_have_valid_flags(self):
        """Test that attribute flags are 'Y' or ''."""
        for elem, attrs in MEASURE_ATTRIBUTES.items():
            self.assertIn(attrs["inputs"], ("Y", ""))
            self.assertIn(attrs["results"], ("Y", ""))

    def test_task_id_element_count(self):
        """Test task ID dimension generates 5000 elements."""
        self.assertEqual(TASK_ID_ELEMENT_COUNT, 5000)

    def test_sample_data_has_both_modes(self):
        """Test sample data includes both stage and optimal mode."""
        self.assertIn("Sample_Optimal_Mode", SAMPLE_DATA)
        self.assertIn("Sample_Stage_Mode", SAMPLE_DATA)

    def test_sample_data_records_have_required_keys(self):
        """Test all sample data records have the expected keys."""
        for workflow, records in SAMPLE_DATA.items():
            for record in records:
                self.assertIn("workflow", record)
                self.assertIn("task_id", record)
                self.assertIn("run_id", record)
                self.assertIn("measure", record)
                self.assertIn("value", record)


class TestDimensionBuilders(unittest.TestCase):
    """Tests for dimension builder functions."""

    def test_workflow_dimension(self):
        """Test workflow dimension has seed elements."""
        dim = _create_workflow_dimension("rushti_workflow")
        self.assertEqual(dim.name, "rushti_workflow")
        hierarchy = dim.hierarchies[0]
        element_names = [e.name for e in hierarchy.elements.values()]
        for seed in WORKFLOW_SEED_ELEMENTS:
            self.assertIn(seed, element_names)

    def test_task_id_dimension(self):
        """Test task ID dimension has 5000 elements."""
        dim = _create_task_id_dimension("rushti_task_id")
        self.assertEqual(dim.name, "rushti_task_id")
        hierarchy = dim.hierarchies[0]
        self.assertEqual(len(hierarchy.elements), TASK_ID_ELEMENT_COUNT)
        # First and last
        element_names = [e.name for e in hierarchy.elements.values()]
        self.assertIn("1", element_names)
        self.assertIn("5000", element_names)

    def test_run_id_dimension(self):
        """Test run ID dimension has seed elements."""
        dim = _create_run_id_dimension("rushti_run_id")
        self.assertEqual(dim.name, "rushti_run_id")
        hierarchy = dim.hierarchies[0]
        element_names = [e.name for e in hierarchy.elements.values()]
        for seed in RUN_ID_SEED_ELEMENTS:
            self.assertIn(seed, element_names)

    def test_measure_dimension(self):
        """Test measure dimension has all elements and attributes."""
        dim = _create_measure_dimension("rushti_measure")
        self.assertEqual(dim.name, "rushti_measure")
        hierarchy = dim.hierarchies[0]
        element_names = [e.name for e in hierarchy.elements.values()]
        for elem in MEASURE_ELEMENTS:
            self.assertIn(elem, element_names)
        # Check attributes defined
        attr_names = [a.name for a in hierarchy.element_attributes]
        self.assertIn("inputs", attr_names)
        self.assertIn("results", attr_names)

    def test_custom_dimension_name(self):
        """Test that custom dimension names are respected."""
        dim = _create_workflow_dimension("my_custom_dim")
        self.assertEqual(dim.name, "my_custom_dim")
        self.assertEqual(dim.hierarchies[0].name, "my_custom_dim")


class TestVerifyLoggingObjects(unittest.TestCase):
    """Tests for verify_logging_objects function."""

    def test_verify_all_exist(self):
        """Test verification when all objects exist."""
        mock_tm1 = Mock()
        mock_tm1.dimensions.exists.return_value = True
        mock_tm1.cubes.exists.return_value = True

        result = verify_logging_objects(mock_tm1)

        self.assertTrue(result[DIM_TASKFILE])
        self.assertTrue(result[DIM_TASK])
        self.assertTrue(result[DIM_RUN])
        self.assertTrue(result[DIM_MEASURE])
        self.assertTrue(result[CUBE_LOGS])

    def test_verify_some_missing(self):
        """Test verification when some objects are missing."""
        mock_tm1 = Mock()

        def exists_side_effect(name):
            return name != DIM_TASKFILE

        mock_tm1.dimensions.exists.side_effect = exists_side_effect
        mock_tm1.cubes.exists.return_value = True

        result = verify_logging_objects(mock_tm1)

        self.assertFalse(result[DIM_TASKFILE])
        self.assertTrue(result[DIM_TASK])
        self.assertTrue(result[CUBE_LOGS])


class TestGetBuildStatus(unittest.TestCase):
    """Tests for get_build_status function."""

    def test_status_all_present(self):
        """Test status message when all objects present."""
        mock_tm1 = Mock()
        mock_tm1.dimensions.exists.return_value = True
        mock_tm1.cubes.exists.return_value = True

        status = get_build_status(mock_tm1)

        self.assertIn("All RushTI logging objects are present", status)

    def test_status_some_missing(self):
        """Test status message when some objects missing."""
        mock_tm1 = Mock()

        def exists_side_effect(name):
            return name != DIM_TASKFILE

        mock_tm1.dimensions.exists.side_effect = exists_side_effect
        mock_tm1.cubes.exists.return_value = True

        status = get_build_status(mock_tm1)

        self.assertIn("Missing:", status)
        self.assertIn(DIM_TASKFILE, status)


class TestBuildLoggingObjects(unittest.TestCase):
    """Tests for build_logging_objects function."""

    @patch("rushti.tm1_build._populate_sample_data")
    @patch("rushti.tm1_build._create_process")
    @patch("rushti.tm1_build._create_mdx_views")
    @patch("rushti.tm1_build._create_mdx_subsets")
    @patch("rushti.tm1_build._create_cube")
    @patch("rushti.tm1_build._create_dimension")
    def test_build_all_objects(
        self,
        mock_create_dim,
        mock_create_cube,
        mock_create_subsets,
        mock_create_views,
        mock_create_process,
        mock_populate,
    ):
        """Test building all logging objects."""
        mock_create_dim.return_value = True
        mock_create_cube.return_value = True
        mock_create_subsets.return_value = {
            "rushti_inputs_opt": True,
            "rushti_inputs_norm": True,
            "rushti_results": True,
        }
        mock_create_views.return_value = {
            "Sample_Normal_Mode": True,
            "Sample_Optimal_Mode": True,
        }
        mock_create_process.return_value = True
        mock_populate.return_value = {"Sample_Stage_Mode": 8, "Sample_Optimal_Mode": 7}

        mock_tm1 = Mock()
        results = build_logging_objects(mock_tm1, force=False)

        # 4 dimensions created
        self.assertEqual(mock_create_dim.call_count, 4)

        # Cube, subsets, views, process, sample data all called
        mock_create_cube.assert_called_once()
        mock_create_subsets.assert_called_once()
        mock_create_views.assert_called_once()
        mock_create_process.assert_called_once()
        mock_populate.assert_called_once()

        # All results True
        self.assertTrue(results[DIM_TASKFILE])
        self.assertTrue(results[DIM_TASK])
        self.assertTrue(results[DIM_RUN])
        self.assertTrue(results[DIM_MEASURE])
        self.assertTrue(results[CUBE_LOGS])

    @patch("rushti.tm1_build._populate_sample_data")
    @patch("rushti.tm1_build._create_process")
    @patch("rushti.tm1_build._create_mdx_views")
    @patch("rushti.tm1_build._create_mdx_subsets")
    @patch("rushti.tm1_build._create_cube")
    @patch("rushti.tm1_build._create_dimension")
    def test_build_handles_dimension_error(
        self,
        mock_create_dim,
        mock_create_cube,
        mock_create_subsets,
        mock_create_views,
        mock_create_process,
        mock_populate,
    ):
        """Test building handles errors gracefully."""
        call_count = 0

        def dimension_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Creation failed")
            return True

        mock_create_dim.side_effect = dimension_side_effect
        mock_create_cube.return_value = True
        mock_create_subsets.return_value = {
            "rushti_inputs_opt": True,
            "rushti_inputs_norm": True,
            "rushti_results": True,
        }
        mock_create_views.return_value = {
            "Sample_Normal_Mode": True,
            "Sample_Optimal_Mode": True,
        }
        mock_create_process.return_value = True
        mock_populate.return_value = {}

        mock_tm1 = Mock()
        results = build_logging_objects(mock_tm1)

        # First dimension (workflow) failed
        self.assertFalse(results[DIM_TASKFILE])
        # Others succeeded
        self.assertTrue(results[DIM_TASK])
        self.assertTrue(results[CUBE_LOGS])


if __name__ == "__main__":
    unittest.main()
