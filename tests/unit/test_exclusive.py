"""
Unit tests for exclusive mode functionality.
Covers context field format, session parsing, wait logic, and session detection.
"""

import unittest
from unittest.mock import MagicMock, patch

from rushti.exclusive import (
    build_session_context,
    parse_session_context,
    should_wait_for_sessions,
    check_active_rushti_sessions,
    wait_for_exclusive_access,
    RushTISession,
    ExclusiveModeTimeoutError,
    TM1_CONTEXT_MAX_LENGTH,
)


class TestBuildSessionContext(unittest.TestCase):
    """Tests for building session context strings."""

    def test_build_context_normal_mode(self):
        """Test building context for normal (non-exclusive) mode."""
        context = build_session_context("daily-etl", exclusive=False)
        self.assertEqual(context, "RushTI_daily-etl")

    def test_build_context_exclusive_mode(self):
        """Test building context for exclusive mode."""
        context = build_session_context("daily-etl", exclusive=True)
        self.assertEqual(context, "RushTIX_daily-etl")

    def test_build_context_truncation(self):
        """Test context is truncated if exceeds TM1 limit."""
        long_id = "a" * 100
        context = build_session_context(long_id, exclusive=False)
        self.assertEqual(len(context), TM1_CONTEXT_MAX_LENGTH)
        self.assertTrue(context.startswith("RushTI_"))

    def test_build_context_empty_id(self):
        """Test building context with empty workflow (no trailing underscore)."""
        context = build_session_context("", exclusive=False)
        self.assertEqual(context, "RushTI")

    def test_build_context_empty_id_exclusive(self):
        """Test building context with empty workflow in exclusive mode."""
        context = build_session_context("", exclusive=True)
        self.assertEqual(context, "RushTIX")

    def test_build_context_special_characters(self):
        """Test context with special characters in workflow."""
        context = build_session_context("task-2024_01", exclusive=False)
        self.assertEqual(context, "RushTI_task-2024_01")


class TestParseSessionContext(unittest.TestCase):
    """Tests for parsing session context strings."""

    def test_parse_normal_mode_context(self):
        """Test parsing normal mode context."""
        result = parse_session_context("RushTI_daily-etl")
        self.assertIsNotNone(result)
        is_exclusive, workflow = result
        self.assertFalse(is_exclusive)
        self.assertEqual(workflow, "_daily-etl")

    def test_parse_exclusive_mode_context(self):
        """Test parsing exclusive mode context."""
        result = parse_session_context("RushTIX_daily-etl")
        self.assertIsNotNone(result)
        is_exclusive, workflow = result
        self.assertTrue(is_exclusive)
        self.assertEqual(workflow, "_daily-etl")

    def test_parse_context_without_underscore(self):
        """Test parsing context without underscore (legacy format)."""
        result = parse_session_context("RushTIdaily-etl")
        self.assertIsNotNone(result)
        is_exclusive, workflow = result
        self.assertFalse(is_exclusive)
        self.assertEqual(workflow, "daily-etl")

    def test_parse_non_rushti_context(self):
        """Test parsing non-RushTI context returns None."""
        result = parse_session_context("SomeOtherApp")
        self.assertIsNone(result)

    def test_parse_empty_context(self):
        """Test parsing empty context returns None."""
        result = parse_session_context("")
        self.assertIsNone(result)

    def test_parse_none_context(self):
        """Test parsing None context returns None."""
        result = parse_session_context(None)
        self.assertIsNone(result)

    def test_parse_rushti_prefix_only(self):
        """Test parsing context with only RushTI prefix."""
        result = parse_session_context("RushTI")
        self.assertIsNotNone(result)
        is_exclusive, workflow = result
        self.assertFalse(is_exclusive)
        self.assertEqual(workflow, "")


class TestShouldWaitForSessions(unittest.TestCase):
    """Tests for exclusive mode wait logic."""

    def test_no_sessions_no_wait(self):
        """Test no wait when no sessions are active."""
        should_wait, blocking = should_wait_for_sessions([], current_exclusive=False)
        self.assertFalse(should_wait)
        self.assertEqual(blocking, [])

    def test_exclusive_run_waits_for_normal_session(self):
        """Test exclusive run waits for non-exclusive sessions."""
        sessions = [
            RushTISession(
                instance_name="tm1srv01", workflow="other-task", is_exclusive=False, thread_id=123
            )
        ]
        should_wait, blocking = should_wait_for_sessions(sessions, current_exclusive=True)
        self.assertTrue(should_wait)
        self.assertEqual(len(blocking), 1)

    def test_exclusive_run_waits_for_exclusive_session(self):
        """Test exclusive run waits for exclusive sessions."""
        sessions = [
            RushTISession(
                instance_name="tm1srv01", workflow="other-task", is_exclusive=True, thread_id=123
            )
        ]
        should_wait, blocking = should_wait_for_sessions(sessions, current_exclusive=True)
        self.assertTrue(should_wait)
        self.assertEqual(len(blocking), 1)

    def test_non_exclusive_waits_for_exclusive(self):
        """Test non-exclusive run waits for exclusive sessions."""
        sessions = [
            RushTISession(
                instance_name="tm1srv01", workflow="other-task", is_exclusive=True, thread_id=123
            )
        ]
        should_wait, blocking = should_wait_for_sessions(sessions, current_exclusive=False)
        self.assertTrue(should_wait)
        self.assertEqual(len(blocking), 1)

    def test_non_exclusive_does_not_wait_for_non_exclusive(self):
        """Test non-exclusive runs can proceed together."""
        sessions = [
            RushTISession(
                instance_name="tm1srv01", workflow="other-task", is_exclusive=False, thread_id=123
            )
        ]
        should_wait, blocking = should_wait_for_sessions(sessions, current_exclusive=False)
        self.assertFalse(should_wait)
        self.assertEqual(blocking, [])

    def test_mixed_sessions_exclusive_blocks(self):
        """Test mixed sessions - exclusive ones block non-exclusive runs."""
        sessions = [
            RushTISession(
                instance_name="tm1srv01", workflow="task1", is_exclusive=False, thread_id=123
            ),
            RushTISession(
                instance_name="tm1srv02", workflow="task2", is_exclusive=True, thread_id=456
            ),
        ]
        should_wait, blocking = should_wait_for_sessions(sessions, current_exclusive=False)
        self.assertTrue(should_wait)
        # Only the exclusive session should be in blocking list
        self.assertEqual(len(blocking), 1)
        self.assertTrue(blocking[0].is_exclusive)

    def test_exclusive_run_blocks_on_multiple_sessions(self):
        """Test exclusive run returns all sessions as blocking."""
        sessions = [
            RushTISession(
                instance_name="tm1srv01", workflow="task1", is_exclusive=False, thread_id=123
            ),
            RushTISession(
                instance_name="tm1srv02", workflow="task2", is_exclusive=False, thread_id=456
            ),
        ]
        should_wait, blocking = should_wait_for_sessions(sessions, current_exclusive=True)
        self.assertTrue(should_wait)
        self.assertEqual(len(blocking), 2)


class TestRushTISession(unittest.TestCase):
    """Tests for RushTISession dataclass."""

    def test_session_string_representation(self):
        """Test session string representation."""
        session = RushTISession(
            instance_name="tm1srv01",
            workflow="daily-etl",
            is_exclusive=True,
            thread_id=123,
            user_name="admin",
        )
        str_repr = str(session)
        self.assertIn("daily-etl", str_repr)
        self.assertIn("exclusive", str_repr)
        self.assertIn("tm1srv01", str_repr)

    def test_session_non_exclusive_string(self):
        """Test session string shows normal mode."""
        session = RushTISession(
            instance_name="tm1srv01", workflow="task1", is_exclusive=False, thread_id=456
        )
        str_repr = str(session)
        self.assertIn("normal", str_repr)


class TestCheckActiveRushTISessions(unittest.TestCase):
    """Tests for check_active_rushti_sessions function with mocked TM1."""

    def _create_mock_tm1_service(self, current_session_id, sessions_data):
        """Create a mock TM1Service with configured session data."""
        mock_tm1 = MagicMock()
        mock_tm1.monitoring.get_current_user.return_value = {"ID": current_session_id}
        mock_tm1.monitoring.get_sessions.return_value = sessions_data
        return mock_tm1

    def test_no_rushti_sessions(self):
        """Test detecting no RushTI sessions when none exist."""
        mock_tm1 = self._create_mock_tm1_service(
            current_session_id=1,
            sessions_data=[
                {
                    "ID": 2,
                    "User": {"Name": "admin"},
                    "Threads": [{"ID": 100, "Context": "SomeOtherApp"}],
                }
            ],
        )
        tm1_services = {"tm1srv01": mock_tm1}

        result = check_active_rushti_sessions(tm1_services)

        self.assertEqual(len(result), 0)

    def test_detect_normal_rushti_session(self):
        """Test detecting a normal (non-exclusive) RushTI session."""
        mock_tm1 = self._create_mock_tm1_service(
            current_session_id=1,
            sessions_data=[
                {
                    "ID": 2,
                    "User": {"Name": "admin"},
                    "Threads": [{"ID": 100, "Context": "RushTI_other-task"}],
                }
            ],
        )
        tm1_services = {"tm1srv01": mock_tm1}

        result = check_active_rushti_sessions(tm1_services)

        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].is_exclusive)
        self.assertEqual(result[0].instance_name, "tm1srv01")

    def test_detect_exclusive_rushti_session(self):
        """Test detecting an exclusive RushTI session."""
        mock_tm1 = self._create_mock_tm1_service(
            current_session_id=1,
            sessions_data=[
                {
                    "ID": 2,
                    "User": {"Name": "admin"},
                    "Threads": [{"ID": 100, "Context": "RushTIX_other-task"}],
                }
            ],
        )
        tm1_services = {"tm1srv01": mock_tm1}

        result = check_active_rushti_sessions(tm1_services)

        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].is_exclusive)

    def test_excludes_own_session_by_id(self):
        """Test that own session is excluded by session ID."""
        mock_tm1 = self._create_mock_tm1_service(
            current_session_id=1,
            sessions_data=[
                {
                    "ID": 1,  # Same as current session
                    "User": {"Name": "admin"},
                    "Threads": [{"ID": 100, "Context": "RushTI_my-task"}],
                },
                {
                    "ID": 2,
                    "User": {"Name": "other"},
                    "Threads": [{"ID": 101, "Context": "RushTI_other-task"}],
                },
            ],
        )
        tm1_services = {"tm1srv01": mock_tm1}

        result = check_active_rushti_sessions(tm1_services)

        # Should only find the other session, not our own
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].workflow, "_other-task")

    def test_excludes_own_context(self):
        """Test that own session context is excluded when specified."""
        mock_tm1 = self._create_mock_tm1_service(
            current_session_id=1,
            sessions_data=[
                {
                    "ID": 2,
                    "User": {"Name": "other"},
                    "Threads": [
                        {"ID": 100, "Context": "RushTI_my-task"},
                        {"ID": 101, "Context": "RushTI_other-task"},
                    ],
                }
            ],
        )
        tm1_services = {"tm1srv01": mock_tm1}

        result = check_active_rushti_sessions(tm1_services, exclude_context="RushTI_my-task")

        # Should only find the other-task context
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].workflow, "_other-task")

    def test_multiple_instances(self):
        """Test checking sessions across multiple TM1 instances."""
        mock_tm1_1 = self._create_mock_tm1_service(
            current_session_id=1,
            sessions_data=[
                {
                    "ID": 2,
                    "User": {"Name": "admin"},
                    "Threads": [{"ID": 100, "Context": "RushTI_task1"}],
                }
            ],
        )
        mock_tm1_2 = self._create_mock_tm1_service(
            current_session_id=3,
            sessions_data=[
                {
                    "ID": 4,
                    "User": {"Name": "admin"},
                    "Threads": [{"ID": 101, "Context": "RushTIX_task2"}],
                }
            ],
        )
        tm1_services = {"tm1srv01": mock_tm1_1, "tm1srv02": mock_tm1_2}

        result = check_active_rushti_sessions(tm1_services)

        self.assertEqual(len(result), 2)
        instances = [s.instance_name for s in result]
        self.assertIn("tm1srv01", instances)
        self.assertIn("tm1srv02", instances)

    def test_handles_session_without_threads(self):
        """Test handling sessions that have no threads."""
        mock_tm1 = self._create_mock_tm1_service(
            current_session_id=1,
            sessions_data=[{"ID": 2, "User": {"Name": "admin"}, "Threads": []}],
        )
        tm1_services = {"tm1srv01": mock_tm1}

        result = check_active_rushti_sessions(tm1_services)

        self.assertEqual(len(result), 0)

    def test_handles_thread_without_context(self):
        """Test handling threads with no context field."""
        mock_tm1 = self._create_mock_tm1_service(
            current_session_id=1,
            sessions_data=[
                {"ID": 2, "User": {"Name": "admin"}, "Threads": [{"ID": 100, "Context": None}]}
            ],
        )
        tm1_services = {"tm1srv01": mock_tm1}

        result = check_active_rushti_sessions(tm1_services)

        self.assertEqual(len(result), 0)

    def test_handles_connection_error(self):
        """Test graceful handling of connection errors."""
        mock_tm1 = MagicMock()
        mock_tm1.monitoring.get_current_user.side_effect = Exception("Connection failed")
        tm1_services = {"tm1srv01": mock_tm1}

        # Should not raise, just return empty list with warning
        result = check_active_rushti_sessions(tm1_services)

        self.assertEqual(len(result), 0)


class TestWaitForExclusiveAccess(unittest.TestCase):
    """Tests for wait_for_exclusive_access function."""

    def _create_mock_tm1_service(self, sessions_data):
        """Create a mock TM1Service."""
        mock_tm1 = MagicMock()
        mock_tm1.monitoring.get_current_user.return_value = {"ID": 1}
        mock_tm1.monitoring.get_sessions.return_value = sessions_data
        return mock_tm1

    def test_no_blocking_sessions_proceeds_immediately(self):
        """Test immediate proceed when no blocking sessions."""
        mock_tm1 = self._create_mock_tm1_service([])
        tm1_services = {"tm1srv01": mock_tm1}

        result = wait_for_exclusive_access(
            tm1_services, current_exclusive=False, session_context="RushTI_my-task"
        )

        self.assertTrue(result)

    def test_force_flag_bypasses_wait(self):
        """Test force flag bypasses exclusive checks."""
        mock_tm1 = self._create_mock_tm1_service(
            [
                {
                    "ID": 2,
                    "User": {"Name": "admin"},
                    "Threads": [{"ID": 100, "Context": "RushTIX_blocking-task"}],
                }
            ]
        )
        tm1_services = {"tm1srv01": mock_tm1}

        result = wait_for_exclusive_access(
            tm1_services, current_exclusive=False, session_context="RushTI_my-task", force=True
        )

        self.assertTrue(result)

    @patch("rushti.exclusive.time.sleep")
    def test_waits_until_sessions_clear(self, mock_sleep):
        """Test waiting until blocking sessions clear."""
        mock_tm1 = MagicMock()
        mock_tm1.monitoring.get_current_user.return_value = {"ID": 1}

        # First call returns blocking session, second call returns empty
        mock_tm1.monitoring.get_sessions.side_effect = [
            [
                {
                    "ID": 2,
                    "User": {"Name": "admin"},
                    "Threads": [{"ID": 100, "Context": "RushTIX_blocking"}],
                }
            ],
            [],
        ]
        tm1_services = {"tm1srv01": mock_tm1}

        result = wait_for_exclusive_access(
            tm1_services, current_exclusive=False, polling_interval=1, timeout=10
        )

        self.assertTrue(result)
        mock_sleep.assert_called_once_with(1)

    @patch("rushti.exclusive.time.sleep")
    def test_timeout_raises_error(self, mock_sleep):
        """Test timeout raises ExclusiveModeTimeoutError."""
        mock_tm1 = MagicMock()
        mock_tm1.monitoring.get_current_user.return_value = {"ID": 1}
        # Always return blocking session
        mock_tm1.monitoring.get_sessions.return_value = [
            {
                "ID": 2,
                "User": {"Name": "admin"},
                "Threads": [{"ID": 100, "Context": "RushTIX_blocking"}],
            }
        ]
        tm1_services = {"tm1srv01": mock_tm1}

        with self.assertRaises(ExclusiveModeTimeoutError) as context:
            wait_for_exclusive_access(
                tm1_services, current_exclusive=False, polling_interval=1, timeout=2
            )

        self.assertIn("Timeout", str(context.exception))
        self.assertIn("blocking", str(context.exception).lower())

    def test_non_exclusive_ignores_non_exclusive_sessions(self):
        """Test non-exclusive run ignores other non-exclusive sessions."""
        mock_tm1 = self._create_mock_tm1_service(
            [
                {
                    "ID": 2,
                    "User": {"Name": "admin"},
                    "Threads": [{"ID": 100, "Context": "RushTI_other-task"}],
                }
            ]
        )
        tm1_services = {"tm1srv01": mock_tm1}

        result = wait_for_exclusive_access(
            tm1_services, current_exclusive=False, session_context="RushTI_my-task"
        )

        self.assertTrue(result)

    def test_exclusive_run_waits_for_non_exclusive(self):
        """Test exclusive run must wait for non-exclusive sessions."""
        mock_tm1 = MagicMock()
        mock_tm1.monitoring.get_current_user.return_value = {"ID": 1}
        mock_tm1.monitoring.get_sessions.return_value = [
            {
                "ID": 2,
                "User": {"Name": "admin"},
                "Threads": [{"ID": 100, "Context": "RushTI_other-task"}],
            }
        ]
        tm1_services = {"tm1srv01": mock_tm1}

        # With short timeout, should raise error since other session blocks
        with self.assertRaises(ExclusiveModeTimeoutError):
            wait_for_exclusive_access(
                tm1_services,
                current_exclusive=True,
                session_context="RushTIX_my-task",
                polling_interval=1,
                timeout=0,  # Immediate timeout
            )


if __name__ == "__main__":
    unittest.main()
