"""Unit tests for early session release in DAG execution.

Tests that TM1 sessions are released as soon as an instance has no remaining
tasks, rather than waiting for the entire workflow to complete.

See: https://github.com/cubewise-code/rushti/issues/135
"""

import asyncio
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

# Path setup handled by conftest.py, but also support direct execution
_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from rushti.dag import DAG  # noqa: E402
from rushti.task import Task  # noqa: E402
from rushti.execution import (  # noqa: E402
    work_through_tasks_dag,
    _logout_instance,
    logout,
    ExecutionContext,
)


def _make_task(task_id, instance="tm1srv01", process="proc"):
    """Helper to create a Task with a specific id and instance."""
    task = Task(instance, process, {})
    task.id = task_id
    return task


class TestGetRemainingTasksByInstance(unittest.TestCase):
    """Tests for DAG.get_remaining_tasks_by_instance()."""

    def test_all_pending(self):
        """All tasks pending: all instances should appear in remaining."""
        dag = DAG()
        dag.add_task(_make_task("1", instance="A"))
        dag.add_task(_make_task("2", instance="B"))
        dag.add_task(_make_task("3", instance="A"))

        remaining = dag.get_remaining_tasks_by_instance()

        self.assertEqual(remaining, {"A": 2, "B": 1})

    def test_some_completed(self):
        """Completed tasks should not appear in remaining counts."""
        dag = DAG()
        t1 = _make_task("1", instance="A")
        t2 = _make_task("2", instance="A")
        t3 = _make_task("3", instance="B")
        dag.add_task(t1)
        dag.add_task(t2)
        dag.add_task(t3)

        dag.mark_running(t1)
        dag.mark_complete(t1, True)

        remaining = dag.get_remaining_tasks_by_instance()

        self.assertEqual(remaining, {"A": 1, "B": 1})

    def test_instance_fully_completed(self):
        """When all tasks for an instance complete, it should not appear."""
        dag = DAG()
        t1 = _make_task("1", instance="A")
        t2 = _make_task("2", instance="B")
        dag.add_task(t1)
        dag.add_task(t2)

        dag.mark_running(t1)
        dag.mark_complete(t1, True)

        remaining = dag.get_remaining_tasks_by_instance()

        self.assertNotIn("A", remaining)
        self.assertEqual(remaining, {"B": 1})

    def test_failed_tasks_not_remaining(self):
        """Failed tasks should not count as remaining."""
        dag = DAG()
        t1 = _make_task("1", instance="A")
        dag.add_task(t1)

        dag.mark_running(t1)
        dag.mark_complete(t1, False)

        remaining = dag.get_remaining_tasks_by_instance()

        self.assertEqual(remaining, {})

    def test_skipped_tasks_not_remaining(self):
        """Skipped tasks should not count as remaining."""
        dag = DAG()
        t1 = _make_task("1", instance="A")
        dag.add_task(t1)

        dag.mark_skipped("1")

        remaining = dag.get_remaining_tasks_by_instance()

        self.assertEqual(remaining, {})

    def test_running_tasks_still_remaining(self):
        """Running tasks should count as remaining."""
        dag = DAG()
        t1 = _make_task("1", instance="A")
        dag.add_task(t1)

        dag.mark_running(t1)

        remaining = dag.get_remaining_tasks_by_instance()

        self.assertEqual(remaining, {"A": 1})

    def test_empty_dag(self):
        """Empty DAG returns empty dict."""
        dag = DAG()
        self.assertEqual(dag.get_remaining_tasks_by_instance(), {})


class TestLogoutInstance(unittest.TestCase):
    """Tests for _logout_instance() helper."""

    def test_logout_calls_logout_and_returns_true(self):
        """Successful logout calls logout, removes the entry, returns True."""
        mock_tm1 = MagicMock()
        services = {"A": mock_tm1, "B": MagicMock()}

        result = _logout_instance("A", services, {}, force=False)

        mock_tm1.logout.assert_called_once()
        self.assertTrue(result)
        # On success the entry is removed so end-of-run logout() can't double-release.
        self.assertNotIn("A", services)
        self.assertIn("B", services)

    def test_logout_failure_keeps_entry_in_services(self):
        """A failed logout leaves the entry so the final cleanup can retry."""
        mock_tm1 = MagicMock()
        mock_tm1.logout.side_effect = Exception("boom")
        services = {"A": mock_tm1}

        result = _logout_instance("A", services, {}, force=False)

        self.assertFalse(result)
        self.assertIn("A", services)

    def test_preserved_connection_skipped_without_force(self):
        """Preserved connections are not logged out without force."""
        mock_tm1 = MagicMock()
        services = {"A": mock_tm1}
        preserve = {"A": True}

        result = _logout_instance("A", services, preserve, force=False)

        mock_tm1.logout.assert_not_called()
        self.assertFalse(result)

    def test_preserved_connection_forced(self):
        """Force mode logs out even preserved connections."""
        mock_tm1 = MagicMock()
        services = {"A": mock_tm1}
        preserve = {"A": True}

        result = _logout_instance("A", services, preserve, force=True)

        mock_tm1.logout.assert_called_once()
        self.assertTrue(result)

    def test_nonexistent_instance_is_noop(self):
        """Calling with a non-existent instance returns False."""
        services = {"B": MagicMock()}

        result = _logout_instance("A", services, {})

        self.assertFalse(result)
        self.assertIn("B", services)

    def test_logout_exception_does_not_raise(self):
        """Logout failure is caught and logged, returns False."""
        mock_tm1 = MagicMock()
        mock_tm1.logout.side_effect = Exception("connection lost")
        services = {"A": mock_tm1}

        result = _logout_instance("A", services, {})

        self.assertFalse(result)


class TestEarlySessionRelease(unittest.TestCase):
    """Tests for early session release during DAG execution."""

    def _build_mock_execute_task(self, sleep_by_instance=None):
        """Build a mock execute_task that optionally sleeps based on instance.

        :param sleep_by_instance: dict of instance_name -> sleep_seconds
        :return: mock function
        """
        sleep_by_instance = sleep_by_instance or {}

        def mock_execute_task(ctx, task, retries, tm1_services):
            sleep_time = sleep_by_instance.get(task.instance_name, 0.01)
            time.sleep(sleep_time)
            return True

        return mock_execute_task

    def _run_dag_with_early_release(
        self,
        dag,
        tm1_services,
        max_workers=4,
        preserve=None,
        force_logout=False,
        sleep_by_instance=None,
    ):
        """Run DAG with early session release enabled."""
        preserve = preserve if preserve is not None else {}
        mock_fn = self._build_mock_execute_task(sleep_by_instance)

        with patch("rushti.execution.execute_task", mock_fn):
            loop = asyncio.new_event_loop()
            try:
                results = loop.run_until_complete(
                    work_through_tasks_dag(
                        ExecutionContext(),
                        dag,
                        max_workers,
                        0,
                        tm1_services,
                        tm1_preserve_connections=preserve,
                        force_logout=force_logout,
                    )
                )
            finally:
                loop.close()
        return results

    def test_multi_instance_early_release(self):
        """Instance A tasks finish first; A is logged out before B tasks complete."""
        dag = DAG()
        # 1 fast task on A, 3 slower tasks on B
        dag.add_task(_make_task("1", instance="A"))
        dag.add_task(_make_task("2", instance="B"))
        dag.add_task(_make_task("3", instance="B"))
        dag.add_task(_make_task("4", instance="B"))

        mock_a = MagicMock()
        mock_b = MagicMock()
        services = {"A": mock_a, "B": mock_b}

        results = self._run_dag_with_early_release(
            dag,
            services,
            max_workers=4,
            sleep_by_instance={"A": 0.01, "B": 0.1},
        )

        self.assertTrue(all(results))
        # A should have been logged out early (after its single task finishes,
        # while B is still running). B is then released after its last task
        # finishes inside the same loop.
        mock_a.logout.assert_called_once()
        mock_b.logout.assert_called_once()
        # On successful early release the entry is removed from tm1_services
        # so that the end-of-run logout() call cannot double-logout the same
        # session (which would emit a CookieConflictError warning).
        self.assertNotIn("A", services)
        self.assertNotIn("B", services)

    def test_single_instance_early_release(self):
        """All tasks on one instance: logout once all tasks complete."""
        dag = DAG()
        dag.add_task(_make_task("1", instance="A"))
        dag.add_task(_make_task("2", instance="A"))

        mock_a = MagicMock()
        services = {"A": mock_a}

        results = self._run_dag_with_early_release(dag, services, max_workers=4)

        self.assertTrue(all(results))
        # A is logged out once after the last task completes
        mock_a.logout.assert_called_once()

    def test_preserved_connection_not_released_early(self):
        """Preserved connections are not released early without force."""
        dag = DAG()
        dag.add_task(_make_task("1", instance="A"))
        dag.add_task(_make_task("2", instance="B"))

        mock_a = MagicMock()
        mock_b = MagicMock()
        services = {"A": mock_a, "B": mock_b}
        preserve = {"A": True}

        results = self._run_dag_with_early_release(
            dag, services, preserve=preserve, force_logout=False
        )

        self.assertTrue(all(results))
        # A is preserved, should not be logged out
        mock_a.logout.assert_not_called()
        self.assertIn("A", services)
        # B should be released
        mock_b.logout.assert_called_once()

    def test_force_logout_releases_preserved(self):
        """Force mode (exclusive) releases even preserved connections."""
        dag = DAG()
        dag.add_task(_make_task("1", instance="A"))
        dag.add_task(_make_task("2", instance="B"))

        mock_a = MagicMock()
        mock_b = MagicMock()
        services = {"A": mock_a, "B": mock_b}
        preserve = {"A": True}

        results = self._run_dag_with_early_release(
            dag, services, preserve=preserve, force_logout=True
        )

        self.assertTrue(all(results))
        # Both should be released even though A is preserved
        mock_a.logout.assert_called_once()
        mock_b.logout.assert_called_once()

    def test_no_early_release_without_preserve_connections(self):
        """No early release when tm1_preserve_connections is not provided.

        This preserves shared connection state for callers (like integration
        tests) that reuse tm1_services across multiple executions.
        """
        dag = DAG()
        dag.add_task(_make_task("1", instance="A"))
        dag.add_task(_make_task("2", instance="B"))

        mock_a = MagicMock()
        mock_b = MagicMock()
        services = {"A": mock_a, "B": mock_b}

        mock_fn = self._build_mock_execute_task()
        with patch("rushti.execution.execute_task", mock_fn):
            loop = asyncio.new_event_loop()
            try:
                results = loop.run_until_complete(
                    work_through_tasks_dag(
                        ExecutionContext(),
                        dag,
                        4,
                        0,
                        services,
                        # tm1_preserve_connections not passed (None) — no early release
                    )
                )
            finally:
                loop.close()

        self.assertTrue(all(results))
        # Neither should be logged out (early release not active)
        mock_a.logout.assert_not_called()
        mock_b.logout.assert_not_called()

    def test_early_release_with_dependencies(self):
        """Early release works correctly with DAG dependencies.

        A1 -> B1 -> B2: Instance A should release after A1 completes,
        even though B tasks depend on it.
        """
        dag = DAG()
        t_a1 = _make_task("1", instance="A")
        t_b1 = _make_task("2", instance="B")
        t_b2 = _make_task("3", instance="B")
        dag.add_task(t_a1)
        dag.add_task(t_b1)
        dag.add_task(t_b2)
        dag.add_dependency("2", "1")  # B1 depends on A1
        dag.add_dependency("3", "2")  # B2 depends on B1

        mock_a = MagicMock()
        mock_b = MagicMock()
        services = {"A": mock_a, "B": mock_b}

        results = self._run_dag_with_early_release(dag, services, max_workers=4)

        self.assertTrue(all(results))
        self.assertEqual(len(results), 3)
        # A should have been released early after A1 completed
        mock_a.logout.assert_called_once()


class TestNoDoubleLogoutAfterEarlyRelease(unittest.TestCase):
    """Regression tests: early release + final logout must not double-logout the same session.

    Before the fix, early session release left the entry in tm1_services
    after calling .logout(). The end-of-run logout() call (from cli.py's
    finally block) then iterated the same dict and called .logout()
    again. The second logout response added a TM1SessionId cookie with
    different (path, domain, secure) attributes than the original — RFC
    6265 says that's a distinct cookie — and TM1py's name-only cookie
    lookup tripped on the duplicate, surfacing as:

        WARNING - Failed to logout from tm1srv01:
                  There are multiple cookies with name, 'TM1SessionId'

    These tests pin the behavior end-to-end: each session is logged out
    exactly once, regardless of whether the release came from the early
    path or the end-of-run path.
    """

    def _build_mock_execute_task(self):
        def mock_execute_task(ctx, task, retries, tm1_services):
            time.sleep(0.005)
            return True

        return mock_execute_task

    def _run_full_lifecycle(self, dag, services, preserve=None, force_logout=False):
        """Run work_through_tasks_dag then the end-of-run logout, like cli.py does."""
        preserve = preserve if preserve is not None else {}
        with patch("rushti.execution.execute_task", self._build_mock_execute_task()):
            loop = asyncio.new_event_loop()
            try:
                results = loop.run_until_complete(
                    work_through_tasks_dag(
                        ExecutionContext(),
                        dag,
                        4,
                        0,
                        services,
                        tm1_preserve_connections=preserve,
                        force_logout=force_logout,
                    )
                )
            finally:
                loop.close()
        # Mirrors the cli.py finally block: logout(tm1_service_by_instance, preserve_connections, ...)
        logout(services, preserve, force=force_logout)
        return results

    def test_single_instance_logged_out_exactly_once(self):
        """Bug scenario: single-instance workflow ran twice through logout pre-fix."""
        dag = DAG()
        dag.add_task(_make_task("1", instance="tm1srv01"))
        dag.add_task(_make_task("2", instance="tm1srv01"))

        mock_tm1 = MagicMock()
        services = {"tm1srv01": mock_tm1}

        self._run_full_lifecycle(dag, services)

        # Pre-fix: called_count == 2 (once in early release, once in final logout).
        # Post-fix: exactly once.
        self.assertEqual(mock_tm1.logout.call_count, 1)
        self.assertNotIn("tm1srv01", services)

    def test_multi_instance_each_logged_out_exactly_once(self):
        """Both early-released and final-released instances logout exactly once."""
        dag = DAG()
        dag.add_task(_make_task("1", instance="A"))
        dag.add_task(_make_task("2", instance="B"))

        mock_a = MagicMock()
        mock_b = MagicMock()
        services = {"A": mock_a, "B": mock_b}

        self._run_full_lifecycle(dag, services)

        self.assertEqual(mock_a.logout.call_count, 1)
        self.assertEqual(mock_b.logout.call_count, 1)
        self.assertEqual(services, {})

    def test_failed_early_release_falls_back_to_final_logout(self):
        """If early release raises, the entry stays so final logout retries.

        This preserves the existing safety net: a transient logout failure
        during early release shouldn't leave the session orphaned.
        """
        dag = DAG()
        dag.add_task(_make_task("1", instance="tm1srv01"))

        mock_tm1 = MagicMock()
        mock_tm1.logout.side_effect = [Exception("transient"), None]
        services = {"tm1srv01": mock_tm1}

        self._run_full_lifecycle(dag, services)

        # Two attempts: failed early release, successful final logout.
        self.assertEqual(mock_tm1.logout.call_count, 2)
        self.assertNotIn("tm1srv01", services)

    def test_preserved_without_force_skips_both_paths(self):
        """A preserved connection without force is skipped by both passes."""
        dag = DAG()
        dag.add_task(_make_task("1", instance="A"))

        mock_a = MagicMock()
        services = {"A": mock_a}
        preserve = {"A": True}

        self._run_full_lifecycle(dag, services, preserve=preserve, force_logout=False)

        mock_a.logout.assert_not_called()
        self.assertIn("A", services)


if __name__ == "__main__":
    unittest.main()
