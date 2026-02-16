"""Integration tests for exclusive mode with real TM1 instances.

These tests require:
- TM1 instance configured in tests/config.ini or RUSHTI_TEST_CONFIG environment variable
- The TM1 instance to support session monitoring

Run with: pytest tests/integration/test_exclusive.py -v -m requires_tm1
"""

import os
import sys
import unittest

import pytest

# Path setup handled by conftest.py, but also support direct execution
_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from TM1py import TM1Service  # noqa: E402

from rushti.exclusive import (  # noqa: E402
    build_session_context,
    parse_session_context,
    check_active_rushti_sessions,
    wait_for_exclusive_access,
)

# Import test config utilities from conftest
from conftest import get_test_tm1_config  # noqa: E402


@pytest.mark.requires_tm1
class TestExclusiveModeIntegration(unittest.TestCase):
    """Integration tests for exclusive mode with real TM1."""

    @classmethod
    def setUpClass(cls):
        """Set up TM1 connection once for all tests."""
        tm1_config, config_source = get_test_tm1_config()
        if tm1_config is None:
            cls.tm1_available = False
            cls.tm1 = None
            cls.tm1_instance = "tm1srv01"
            return

        cls.tm1_instance = tm1_config.instance
        try:
            cls.tm1 = TM1Service(**tm1_config.to_dict())
            cls.tm1_available = True
        except Exception as e:
            print(f"TM1 connection failed: {e}")
            cls.tm1_available = False
            cls.tm1 = None

    @classmethod
    def tearDownClass(cls):
        """Clean up TM1 connection."""
        if cls.tm1:
            try:
                cls.tm1.logout()
            except Exception:
                pass

    def setUp(self):
        if not self.tm1_available:
            self.skipTest("TM1 instance not available")

    def test_get_sessions_with_threads(self):
        """Test that get_sessions with include_threads=True works."""
        sessions = self.tm1.monitoring.get_sessions(include_threads=True)

        # Should return a list
        self.assertIsInstance(sessions, list)

        # At least our own session should be present
        self.assertGreaterEqual(len(sessions), 1)

        # Check structure of session data
        for session in sessions:
            if isinstance(session, dict):
                self.assertIn("ID", session)
                # Threads may be present
                if "Threads" in session:
                    self.assertIsInstance(session["Threads"], list)

    def test_get_current_user(self):
        """Test that get_current_user returns session info."""
        current = self.tm1.monitoring.get_current_user()

        # Should return something (User object, dict, or other)
        self.assertIsNotNone(current)

        # The return type varies by TM1py version, so just verify it's usable
        # The exclusive module handles both dict and object forms

    def test_check_active_sessions_excludes_own(self):
        """Test that check_active_rushti_sessions excludes our own session."""
        # Build our session context
        context = build_session_context("test-exclusive", exclusive=False)

        tm1_services = {self.tm1_instance: self.tm1}

        # Check for active sessions, excluding our context
        sessions = check_active_rushti_sessions(tm1_services, exclude_context=context)

        # Our session should not appear in results (even if it has RushTI context)
        for session in sessions:
            self.assertNotEqual(session.workflow, "_test-exclusive")

    def test_wait_for_exclusive_no_blocking(self):
        """Test wait_for_exclusive_access proceeds when no blocking sessions."""
        context = build_session_context("test-wait", exclusive=False)
        tm1_services = {self.tm1_instance: self.tm1}

        # Should proceed immediately (no other RushTI sessions)
        result = wait_for_exclusive_access(
            tm1_services,
            current_exclusive=False,
            session_context=context,
            polling_interval=1,
            timeout=5,
        )

        self.assertTrue(result)

    def test_force_flag_with_real_tm1(self):
        """Test force flag works with real TM1 connection."""
        context = build_session_context("test-force", exclusive=True)
        tm1_services = {self.tm1_instance: self.tm1}

        # Even with exclusive mode, force should proceed
        result = wait_for_exclusive_access(
            tm1_services, current_exclusive=True, session_context=context, force=True
        )

        self.assertTrue(result)

    def test_session_context_format(self):
        """Test that session context format is consistent."""
        # Normal mode
        normal_context = build_session_context("my-task", exclusive=False)
        self.assertEqual(normal_context, "RushTI_my-task")

        # Exclusive mode
        exclusive_context = build_session_context("my-task", exclusive=True)
        self.assertEqual(exclusive_context, "RushTIX_my-task")

        # Both should parse correctly
        normal_parsed = parse_session_context(normal_context)
        self.assertIsNotNone(normal_parsed)
        self.assertFalse(normal_parsed[0])  # is_exclusive

        exclusive_parsed = parse_session_context(exclusive_context)
        self.assertIsNotNone(exclusive_parsed)
        self.assertTrue(exclusive_parsed[0])  # is_exclusive


@pytest.mark.requires_tm1
class TestMultiInstanceExclusiveMode(unittest.TestCase):
    """Test exclusive mode across multiple TM1 instances.

    Note: These tests require multiple TM1 instances configured.
    They will be skipped if only one instance is available.
    """

    @classmethod
    def setUpClass(cls):
        """Set up connections to available TM1 instances."""
        cls.tm1_services = {}
        cls.config_path = None

        tm1_config, config_source = get_test_tm1_config()
        if tm1_config is None:
            cls.tm1_available = False
            return

        cls.config_path = config_source

        try:
            tm1 = TM1Service(**tm1_config.to_dict())
            cls.tm1_services[tm1_config.instance] = tm1
            cls.tm1_available = True
        except Exception as e:
            print(f"TM1 connection failed: {e}")
            cls.tm1_available = False

    @classmethod
    def tearDownClass(cls):
        """Clean up all TM1 connections."""
        for tm1 in cls.tm1_services.values():
            try:
                tm1.logout()
            except Exception:
                pass

    def setUp(self):
        if not self.tm1_available:
            self.skipTest("TM1 instance not available")

    def test_check_sessions_single_instance(self):
        """Test session checking works with single instance."""
        context = build_session_context("multi-test", exclusive=False)

        sessions = check_active_rushti_sessions(self.tm1_services, exclude_context=context)

        # Should return a list (may be empty if no other RushTI sessions)
        self.assertIsInstance(sessions, list)

    def test_exclusive_access_single_instance(self):
        """Test exclusive access with single instance."""
        context = build_session_context("multi-exclusive", exclusive=True)

        result = wait_for_exclusive_access(
            self.tm1_services,
            current_exclusive=True,
            session_context=context,
            polling_interval=1,
            timeout=5,
        )

        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
