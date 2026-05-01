"""Phase 3 safety-net: StatsRepository Protocol parity smoke test.

These tests pin the structural-typing seam introduced in Phase 3:

- Both real adapters (``StatsDatabase``, ``DynamoDBStatsDatabase``)
  satisfy the ``StatsRepository`` Protocol structurally.
- A small fake adapter is also a valid ``StatsRepository`` and is
  enough to drive ``TaskOptimizer`` end-to-end without touching
  SQLite or DynamoDB. This is the proof point that the seam is useful:
  optimizer tests no longer have to spin up a real database.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest import TestCase

from rushti.optimizer import TaskOptimizer
from rushti.settings import OptimizationSettings
from rushti.stats import (
    DynamoDBStatsDatabase,
    StatsDatabase,
    StatsRepository,
    calculate_task_signature,
)


class FakeStatsRepository:
    """In-memory stats repository for fast tests.

    Implements the full ``StatsRepository`` Protocol surface so it can
    be swapped in anywhere a SQLite/DynamoDB adapter is expected.
    Methods that aren't exercised by the optimizer test path return
    empty/no-op defaults rather than raising — that keeps the fake
    safe to pass through code paths that defensively probe the
    repository (e.g., cleanup_old_data on construction).
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._history: Dict[str, List[Dict[str, Any]]] = {}

    def seed_history(
        self, signature: str, durations_seconds: List[float], success: bool = True
    ) -> None:
        """Inject historical task results for one signature."""
        base = datetime(2026, 4, 1, 9, 0, 0)
        records = []
        for i, dur in enumerate(durations_seconds):
            start = base + timedelta(minutes=i * 10)
            end = start + timedelta(seconds=dur)
            records.append(
                {
                    "task_signature": signature,
                    "duration_seconds": dur,
                    "success": success,
                    "start_time": start.isoformat(),
                    "end_time": end.isoformat(),
                    "retry_count": 0,
                }
            )
        self._history[signature] = records

    # ---- write path (no-ops for the fake) ----

    def start_run(self, run_id, workflow, **_kwargs) -> None:
        return None

    def record_task(self, *_args, **_kwargs) -> None:
        return None

    def batch_record_tasks(self, tasks: List[Dict[str, Any]]) -> None:
        return None

    def complete_run(self, run_id, status="Success", **_kwargs) -> None:
        return None

    def cleanup_old_data(self, retention_days: int) -> int:
        return 0

    # ---- read path ----

    def get_task_history(self, task_signature: str, limit: int = 10) -> List[Dict[str, Any]]:
        return list(self._history.get(task_signature, []))[:limit]

    def get_workflow_signatures(self, workflow: str) -> List[str]:
        return list(self._history.keys())

    def get_task_sample_count(self, task_signature: str) -> int:
        return len(self._history.get(task_signature, []))

    def get_task_durations(self, task_signature: str, limit: int = 10) -> List[float]:
        return [r["duration_seconds"] for r in self._history.get(task_signature, [])[:limit]]

    def get_run_results(self, run_id: str) -> List[Dict[str, Any]]:
        return []

    def get_run_info(self, run_id: str):
        return None

    def get_runs_for_workflow(self, workflow: str) -> List[Dict[str, Any]]:
        return []

    def get_all_runs(self) -> List[Dict[str, Any]]:
        return []

    def get_run_task_stats(self, run_id: str):
        return None

    def get_concurrent_task_counts(self, run_id: str) -> List[Dict[str, Any]]:
        return []

    # ---- lifecycle ----

    def close(self) -> None:
        return None

    def __enter__(self) -> "FakeStatsRepository":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class TestStatsRepositoryProtocol(TestCase):
    """Both real adapters and the fake satisfy the Protocol."""

    def test_sqlite_adapter_satisfies_protocol(self):
        db = StatsDatabase(enabled=False)
        # runtime_checkable Protocol — instance check verifies method
        # names are present without requiring inheritance.
        self.assertIsInstance(db, StatsRepository)

    def test_dynamodb_adapter_satisfies_protocol(self):
        # We don't connect to real AWS — DynamoDBStatsDatabase with
        # enabled=False is a no-op shell that still exposes the surface.
        db = DynamoDBStatsDatabase(enabled=False)
        self.assertIsInstance(db, StatsRepository)

    def test_fake_repository_satisfies_protocol(self):
        fake = FakeStatsRepository(enabled=True)
        self.assertIsInstance(fake, StatsRepository)


class TestOptimizerWithFakeRepository(TestCase):
    """The fake adapter is enough to drive TaskOptimizer."""

    def test_estimate_uses_history_from_fake_repository(self):
        signature = calculate_task_signature(
            instance="tm1srv01",
            process="bedrock.server.wait",
            parameters={"pWaitSec": "1"},
        )

        fake = FakeStatsRepository(enabled=True)
        fake.seed_history(signature, durations_seconds=[10.0, 12.0, 11.0, 13.0])

        optimizer = TaskOptimizer(
            stats_db=fake,
            settings=OptimizationSettings(min_samples=2, lookback_runs=4),
            workflow="phase3-fake-workflow",
            algorithm="longest_first",
        )

        estimate = optimizer._calculate_estimate(signature)

        # Estimate falls in a sensible neighborhood of the seeded samples;
        # we don't pin the exact EWMA output to avoid coupling to algo details.
        self.assertGreater(estimate.ewma_duration, 8.0)
        self.assertLess(estimate.ewma_duration, 15.0)
        self.assertFalse(estimate.estimated)
        self.assertEqual(estimate.sample_count, 4)

    def test_estimate_falls_back_when_no_history(self):
        fake = FakeStatsRepository(enabled=True)
        # No history seeded -> optimizer must mark the estimate as
        # `estimated=True` rather than crash on the empty-list path.
        optimizer = TaskOptimizer(
            stats_db=fake,
            settings=OptimizationSettings(min_samples=2, lookback_runs=4),
            workflow="phase3-fake-workflow",
            algorithm="longest_first",
        )

        estimate = optimizer._calculate_estimate("nonexistent_signature")

        self.assertTrue(estimate.estimated)
        self.assertEqual(estimate.sample_count, 0)
