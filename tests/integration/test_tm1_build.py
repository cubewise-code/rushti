"""Integration tests for TM1 build module with real TM1 instance.

Tests tm1_build module by creating actual TM1 objects (dimensions, cubes)
on a test TM1 instance to verify end-to-end functionality.
"""

import os
import sys
import unittest

import pytest

# Add src to path for imports
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
    ),
)

from TM1py import TM1Service

from rushti.tm1_build import (
    build_logging_objects,
    get_build_status,
    _populate_sample_data,
    verify_logging_objects,
)
from conftest import get_test_tm1_names

# Load dimension and cube names from test settings (supports _test suffixes)
_TM1_NAMES = get_test_tm1_names()
DIM_TASKFILE = _TM1_NAMES["dim_workflow"]
DIM_TASK = _TM1_NAMES["dim_task"]
DIM_RUN = _TM1_NAMES["dim_run"]
DIM_MEASURE = _TM1_NAMES["dim_measure"]
CUBE_LOGS = _TM1_NAMES["cube_name"]


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestTM1BuildIntegration(unittest.TestCase):
    """Integration tests for TM1 build operations."""

    @classmethod
    def setUpClass(cls):
        """Set up TM1 connection for all tests."""
        # Get TM1 config from conftest
        from conftest import get_test_tm1_config

        cls.tm1_config, source = get_test_tm1_config()
        if cls.tm1_config is None:
            pytest.skip(f"TM1 not available: {source}")

        cls.tm1 = TM1Service(**cls.tm1_config.to_dict())
        cls.instance_name = cls.tm1_config.instance

        # Clean up any existing test objects
        cls._cleanup_test_objects()

    @classmethod
    def tearDownClass(cls):
        """Clean up and close TM1 connection."""
        if hasattr(cls, "tm1"):
            cls._cleanup_test_objects()
            cls.tm1.logout()

    @classmethod
    def _cleanup_test_objects(cls):
        """Remove test TM1 objects if they exist."""
        if not hasattr(cls, "tm1"):
            return

        try:
            # Delete cube first (depends on dimensions)
            if cls.tm1.cubes.exists(CUBE_LOGS):
                cls.tm1.cubes.delete(CUBE_LOGS)

            # Delete dimensions
            for dim in [DIM_TASKFILE, DIM_TASK, DIM_RUN, DIM_MEASURE]:
                if cls.tm1.dimensions.exists(dim):
                    cls.tm1.dimensions.delete(dim)
        except Exception:
            pass  # Ignore cleanup errors

    def test_build_all_objects_from_scratch(self):
        """Test building all logging objects from scratch."""
        # Ensure clean state
        self._cleanup_test_objects()

        results = build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        # Verify all objects were created
        self.assertTrue(results[DIM_TASKFILE], "Taskfile dimension should be created")
        self.assertTrue(results[DIM_TASK], "Task dimension should be created")
        self.assertTrue(results[DIM_RUN], "Run dimension should be created")
        self.assertTrue(results[DIM_MEASURE], "Measure dimension should be created")
        self.assertTrue(results[CUBE_LOGS], "Logs cube should be created")

        # Verify objects actually exist in TM1
        verification = verify_logging_objects(self.tm1, **_TM1_NAMES)
        self.assertTrue(all(verification.values()), "All objects should exist after build")

    def test_build_objects_already_exist(self):
        """Test building when objects already exist (should not recreate without force)."""
        # First build
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        # Second build without force
        results = build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        # Should return False for existing objects
        self.assertFalse(results[DIM_TASKFILE], "Should not recreate existing dimension")
        self.assertFalse(results[CUBE_LOGS], "Should not recreate existing cube")

    def test_build_with_force_flag(self):
        """Test building with force flag recreates objects."""
        # First build
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        # Force rebuild
        results = build_logging_objects(self.tm1, force=True, **_TM1_NAMES)

        # Should return True (recreated)
        self.assertTrue(results[DIM_TASKFILE], "Should recreate dimension with force")
        self.assertTrue(results[CUBE_LOGS], "Should recreate cube with force")

        # Objects should still exist
        verification = verify_logging_objects(self.tm1, **_TM1_NAMES)
        self.assertTrue(all(verification.values()))

    def test_taskfile_dimension_structure(self):
        """Test taskfile dimension has correct structure."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        # Check dimension exists with sample elements
        hierarchy = self.tm1.dimensions.hierarchies.get(DIM_TASKFILE, DIM_TASKFILE)

        self.assertIsNotNone(hierarchy)
        element_names = list(hierarchy.elements.keys())

        # Should have sample taskfile elements
        self.assertIn("Sample_Stage_Mode", element_names)
        self.assertIn("Sample_Optimal_Mode", element_names)

    def test_task_dimension_structure(self):
        """Test task dimension has correct structure."""
        from rushti.tm1_objects import TASK_ID_ELEMENT_COUNT

        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        hierarchy = self.tm1.dimensions.hierarchies.get(DIM_TASK, DIM_TASK)

        self.assertIsNotNone(hierarchy)
        element_count = len(hierarchy.elements)

        # Should have the default number of task elements
        self.assertEqual(element_count, TASK_ID_ELEMENT_COUNT)

        # Check elements are numeric strings
        self.assertIn("1", list(hierarchy.elements.keys()))
        self.assertIn(str(TASK_ID_ELEMENT_COUNT), list(hierarchy.elements.keys()))

    def test_run_dimension_structure(self):
        """Test run dimension has correct structure."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        hierarchy = self.tm1.dimensions.hierarchies.get(DIM_RUN, DIM_RUN)

        self.assertIsNotNone(hierarchy)
        # Should have at least "Input" element
        self.assertIn("Input", list(hierarchy.elements.keys()))

    def test_measure_dimension_structure(self):
        """Test measure dimension has correct structure."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        hierarchy = self.tm1.dimensions.hierarchies.get(DIM_MEASURE, DIM_MEASURE)

        self.assertIsNotNone(hierarchy)
        element_names = list(hierarchy.elements.keys())

        # Check required measures
        required_measures = [
            "instance",
            "process",
            "parameters",
            "status",
            "predecessors",
            "stage",
            "safe_retry",
            "timeout",
        ]

        for measure in required_measures:
            self.assertIn(measure, element_names, f"Measure '{measure}' should exist")

        # Check rushti_inputs subsets exist
        subsets = self.tm1.subsets.get_all_names(DIM_MEASURE, DIM_MEASURE, private=False)
        self.assertIn("rushti_inputs_opt", subsets)
        self.assertIn("rushti_inputs_norm", subsets)

    def test_cube_structure(self):
        """Test logs cube has correct structure."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        cube = self.tm1.cubes.get(CUBE_LOGS)

        self.assertIsNotNone(cube)
        # Check cube has correct dimensions in order
        expected_dims = [DIM_TASKFILE, DIM_RUN, DIM_TASK, DIM_MEASURE]
        self.assertEqual(cube.dimensions, expected_dims)

    def test_populate_sample_data(self):
        """Test populating sample taskfile data."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        results = _populate_sample_data(self.tm1, CUBE_LOGS)

        # Should populate both sample taskfiles
        self.assertIn("Sample_Stage_Mode", results)
        self.assertIn("Sample_Optimal_Mode", results)

        # Should have created tasks
        self.assertGreater(results["Sample_Stage_Mode"], 0)
        self.assertGreater(results["Sample_Optimal_Mode"], 0)

        # Verify data exists in cube
        # Check first task of Sample_Stage_Mode
        instance = self.tm1.cells.get_value(CUBE_LOGS, "Sample_Stage_Mode,Input,1,instance")
        self.assertIsNotNone(instance)
        self.assertEqual(instance, "tm1srv01")

        # Check process name
        process = self.tm1.cells.get_value(CUBE_LOGS, "Sample_Stage_Mode,Input,1,process")
        self.assertIsNotNone(process)
        self.assertIn("bedrock", process.lower())

    def test_verify_logging_objects(self):
        """Test verification function correctly identifies existing objects."""
        # Clean state
        self._cleanup_test_objects()

        # Before build - nothing should exist
        verification = verify_logging_objects(self.tm1, **_TM1_NAMES)
        self.assertFalse(any(verification.values()))

        # After build - everything should exist
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)
        verification = verify_logging_objects(self.tm1, **_TM1_NAMES)
        self.assertTrue(all(verification.values()))

    def test_get_build_status(self):
        """Test build status reporting."""
        # Clean state
        self._cleanup_test_objects()

        # Before build
        status = get_build_status(self.tm1, **_TM1_NAMES)
        self.assertIn("Missing:", status)

        # After build
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)
        status = get_build_status(self.tm1, **_TM1_NAMES)
        self.assertIn("All RushTI logging objects are present", status)

    def test_rebuild_preserves_custom_data(self):
        """Test that rebuild with force preserves custom taskfile elements."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        # Add custom taskfile element
        from TM1py.Objects import Element

        custom_element = Element("custom-taskfile", "String")
        self.tm1.dimensions.hierarchies.elements.create(DIM_TASKFILE, DIM_TASKFILE, custom_element)

        # Verify element exists
        hierarchy = self.tm1.dimensions.hierarchies.get(DIM_TASKFILE, DIM_TASKFILE)
        self.assertIn("custom-taskfile", list(hierarchy.elements.keys()))

        # Rebuild without force (should not affect custom element)
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)

        # Custom element should still exist
        hierarchy = self.tm1.dimensions.hierarchies.get(DIM_TASKFILE, DIM_TASKFILE)
        self.assertIn("custom-taskfile", list(hierarchy.elements.keys()))

    def test_sample_data_has_valid_structure(self):
        """Test that sample data has valid task structure."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)
        _populate_sample_data(self.tm1, CUBE_LOGS)

        # Read Sample_Optimal_Mode data
        mdx = f"""
        SELECT
            NON EMPTY {{[{DIM_MEASURE}].[{DIM_MEASURE}].[instance],
                       [{DIM_MEASURE}].[{DIM_MEASURE}].[process],
                       [{DIM_MEASURE}].[{DIM_MEASURE}].[parameters]}} ON COLUMNS,
            NON EMPTY {{[{DIM_TASK}].[{DIM_TASK}].[1]}} ON ROWS
        FROM [{CUBE_LOGS}]
        WHERE ([{DIM_RUN}].[{DIM_RUN}].[Input],
               [{DIM_TASKFILE}].[{DIM_TASKFILE}].[Sample_Optimal_Mode])
        """

        df = self.tm1.cells.execute_mdx_dataframe_shaped(mdx)

        self.assertFalse(df.empty, "Should have data for task 1")

        # Check values
        self.assertIn("instance", df.columns)
        self.assertIn("process", df.columns)
        self.assertIn("parameters", df.columns)


if __name__ == "__main__":
    unittest.main()
