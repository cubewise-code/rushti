"""Integration tests for TM1 build command on both v11 and v12 instances.

Tests build_logging_objects, verify_logging_objects, and auto-created test
objects (counter dimension, load results process) on both TM1 versions.

Run with: pytest tests/integration/test_v11_v12_build.py -v -m requires_tm1
"""

import os
import sys
import unittest

import pytest

_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)
_integration_path = os.path.dirname(os.path.abspath(__file__))
if _integration_path not in sys.path:
    sys.path.insert(0, _integration_path)

from TM1py import TM1Service

from rushti.tm1_build import (
    build_logging_objects,
    verify_logging_objects,
    _populate_sample_data,
)
from tm1_setup import (
    ensure_counter_dimension,
    ensure_load_results_process,
    ensure_rushti_cube,
)
from conftest import get_all_test_tm1_configs, get_test_tm1_names

# Load dimension/cube names from test settings (supports _test suffixes)
_TM1_NAMES = get_test_tm1_names()
DIM_WORKFLOW = _TM1_NAMES["dim_workflow"]
DIM_TASK = _TM1_NAMES["dim_task"]
DIM_RUN = _TM1_NAMES["dim_run"]
DIM_MEASURE = _TM1_NAMES["dim_measure"]
CUBE_RUSHTI = _TM1_NAMES["cube_name"]
DIM_COUNTER = "rushti.dimension.counter"


def _get_services():
    """Connect to all configured TM1 instances."""
    configs, _ = get_all_test_tm1_configs()
    services = {}
    for instance, config in configs.items():
        try:
            tm1 = TM1Service(**config.to_dict())
            tm1.server.get_server_name()
            services[instance] = tm1
        except Exception:
            pass
    return services


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestBuildOnV11(unittest.TestCase):
    """Build command tests for TM1 v11 (tm1srv01)."""

    INSTANCE = "tm1srv01"

    @classmethod
    def setUpClass(cls):
        services = _get_services()
        if cls.INSTANCE not in services:
            cls.tm1 = None
            # Close other connections
            for tm1 in services.values():
                try:
                    tm1.logout()
                except Exception:
                    pass
            return
        cls.tm1 = services[cls.INSTANCE]
        # Close other connections we don't need
        for name, tm1 in services.items():
            if name != cls.INSTANCE:
                try:
                    tm1.logout()
                except Exception:
                    pass

    @classmethod
    def tearDownClass(cls):
        if cls.tm1:
            try:
                cls.tm1.logout()
            except Exception:
                pass

    def setUp(self):
        if self.tm1 is None:
            self.skipTest(f"{self.INSTANCE} not available")

    def test_build_creates_all_objects(self):
        """Build creates all rushti objects on v11."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)
        verification = verify_logging_objects(self.tm1, **_TM1_NAMES)
        self.assertTrue(all(verification.values()), f"Missing objects: {verification}")

    def test_build_force_recreates(self):
        """Force build recreates objects on v11."""
        results = build_logging_objects(self.tm1, force=True, **_TM1_NAMES)
        self.assertTrue(results[DIM_WORKFLOW])
        self.assertTrue(results[CUBE_RUSHTI])
        verification = verify_logging_objects(self.tm1, **_TM1_NAMES)
        self.assertTrue(all(verification.values()))

    def test_sample_data_populated(self):
        """Sample data is populated on v11."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)
        results = _populate_sample_data(self.tm1, CUBE_RUSHTI)
        self.assertIn("Sample_Stage_Mode", results)
        self.assertGreater(results["Sample_Stage_Mode"], 0)

    def test_counter_dimension_created(self):
        """Counter dimension is created on v11."""
        ensure_counter_dimension(self.tm1)
        self.assertTrue(self.tm1.dimensions.exists(DIM_COUNTER))
        hierarchy = self.tm1.dimensions.hierarchies.get(DIM_COUNTER, DIM_COUNTER)
        self.assertIn("1", list(hierarchy.elements.keys()))
        self.assertIn("10", list(hierarchy.elements.keys()))

    def test_load_results_process_created(self):
        """Load results process is created on v11."""
        ensure_rushti_cube(self.tm1, **_TM1_NAMES)
        ensure_load_results_process(self.tm1)
        self.assertTrue(self.tm1.processes.exists("}rushti.load.results"))


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestBuildOnV12(unittest.TestCase):
    """Build command tests for TM1 v12 (tm1srv02)."""

    INSTANCE = "tm1srv02"

    @classmethod
    def setUpClass(cls):
        services = _get_services()
        if cls.INSTANCE not in services:
            cls.tm1 = None
            for tm1 in services.values():
                try:
                    tm1.logout()
                except Exception:
                    pass
            return
        cls.tm1 = services[cls.INSTANCE]
        for name, tm1 in services.items():
            if name != cls.INSTANCE:
                try:
                    tm1.logout()
                except Exception:
                    pass

    @classmethod
    def tearDownClass(cls):
        if cls.tm1:
            try:
                cls.tm1.logout()
            except Exception:
                pass

    def setUp(self):
        if self.tm1 is None:
            self.skipTest(f"{self.INSTANCE} not available")

    def test_build_creates_all_objects(self):
        """Build creates all rushti objects on v12."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)
        verification = verify_logging_objects(self.tm1, **_TM1_NAMES)
        self.assertTrue(all(verification.values()), f"Missing objects: {verification}")

    def test_build_force_recreates(self):
        """Force build recreates objects on v12."""
        results = build_logging_objects(self.tm1, force=True, **_TM1_NAMES)
        self.assertTrue(results[DIM_WORKFLOW])
        self.assertTrue(results[CUBE_RUSHTI])
        verification = verify_logging_objects(self.tm1, **_TM1_NAMES)
        self.assertTrue(all(verification.values()))

    def test_sample_data_populated(self):
        """Sample data is populated on v12."""
        build_logging_objects(self.tm1, force=False, **_TM1_NAMES)
        results = _populate_sample_data(self.tm1, CUBE_RUSHTI)
        self.assertIn("Sample_Stage_Mode", results)
        self.assertGreater(results["Sample_Stage_Mode"], 0)

    def test_counter_dimension_created(self):
        """Counter dimension is created on v12."""
        ensure_counter_dimension(self.tm1)
        self.assertTrue(self.tm1.dimensions.exists(DIM_COUNTER))
        hierarchy = self.tm1.dimensions.hierarchies.get(DIM_COUNTER, DIM_COUNTER)
        self.assertIn("1", list(hierarchy.elements.keys()))
        self.assertIn("10", list(hierarchy.elements.keys()))

    def test_load_results_process_created(self):
        """Load results process is created on v12."""
        ensure_rushti_cube(self.tm1, **_TM1_NAMES)
        ensure_load_results_process(self.tm1)
        self.assertTrue(self.tm1.processes.exists("}rushti.load.results"))


if __name__ == "__main__":
    unittest.main()
