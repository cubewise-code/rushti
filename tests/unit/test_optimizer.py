"""Unit tests for the optimizer module."""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from unittest import TestCase
from unittest.mock import Mock, patch

from rushti.optimizer import (
    RuntimeEstimate,
    TaskOptimizer,
    create_task_optimizer,
    VALID_ALGORITHMS,
)
from rushti.settings import OptimizationSettings
from rushti.stats import StatsDatabase, calculate_task_signature


@dataclass
class MockTask:
    """Mock task for testing."""

    id: str
    instance_name: str
    process_name: str
    parameters: Optional[Dict[str, Any]] = None


class TestRuntimeEstimate(TestCase):
    """Tests for RuntimeEstimate dataclass."""

    def test_create_estimate(self):
        """Can create a RuntimeEstimate."""
        estimate = RuntimeEstimate(
            task_signature="abc123",
            ewma_duration=10.5,
            sample_count=5,
            confidence=0.75,
            estimated=False,
        )
        self.assertEqual(estimate.task_signature, "abc123")
        self.assertEqual(estimate.ewma_duration, 10.5)
        self.assertEqual(estimate.sample_count, 5)
        self.assertEqual(estimate.confidence, 0.75)
        self.assertFalse(estimate.estimated)

    def test_estimated_flag(self):
        """Estimated flag indicates task has no history."""
        estimate = RuntimeEstimate(
            task_signature="abc123",
            ewma_duration=10.0,
            sample_count=0,
            confidence=0.0,
            estimated=True,
        )
        self.assertTrue(estimate.estimated)
        self.assertEqual(estimate.sample_count, 0)


class TestTaskOptimizerEWMA(TestCase):
    """Tests for EWMA calculation in TaskOptimizer."""

    def setUp(self):
        """Set up mock stats database and settings."""
        self.stats_db = Mock(spec=StatsDatabase)
        self.stats_db.enabled = True
        self.stats_db.get_task_history.return_value = []

        self.settings = OptimizationSettings(
            lookback_runs=10,
            time_of_day_weighting=False,
            min_samples=3,
            cache_duration_hours=24,
        )

        self.optimizer = TaskOptimizer(
            stats_db=self.stats_db,
            settings=self.settings,
            workflow="test_taskfile",
            algorithm="longest_first",
        )

    def test_ewma_single_value(self):
        """EWMA with single value returns that value."""
        result = self.optimizer._calculate_ewma([100.0])
        self.assertEqual(result, 100.0)

    def test_ewma_two_values(self):
        """EWMA with two values blends them."""
        # With alpha=0.3: ewma = 0.3 * old + 0.7 * new
        # Start with 100, then process 50
        # ewma = 0.3 * 50 + 0.7 * 100 = 15 + 70 = 85
        result = self.optimizer._calculate_ewma([100.0, 50.0])
        self.assertAlmostEqual(result, 85.0, places=1)

    def test_ewma_outlier_dampening(self):
        """Outliers > 3x current estimate are capped at 2x."""
        # Start with 100, then process 400 (> 3 * 100)
        # Dampened to 200 (2 * 100)
        # ewma = 0.3 * 200 + 0.7 * 100 = 60 + 70 = 130
        result = self.optimizer._calculate_ewma([100.0, 400.0])
        self.assertAlmostEqual(result, 130.0, places=1)

    def test_ewma_empty_list(self):
        """EWMA with empty list returns 0."""
        result = self.optimizer._calculate_ewma([])
        self.assertEqual(result, 0.0)

    def test_ewma_respects_alpha(self):
        """EWMA calculation respects alpha parameter."""
        # With alpha=0.5: more weight on older values
        result = self.optimizer._calculate_ewma([100.0, 50.0], alpha=0.5)
        # ewma = 0.5 * 50 + 0.5 * 100 = 75
        self.assertAlmostEqual(result, 75.0, places=1)


class TestTaskOptimizerConfidence(TestCase):
    """Tests for confidence calculation in TaskOptimizer."""

    def setUp(self):
        """Set up optimizer."""
        self.stats_db = Mock(spec=StatsDatabase)
        self.stats_db.enabled = True

        self.settings = OptimizationSettings()

        self.optimizer = TaskOptimizer(
            stats_db=self.stats_db,
            settings=self.settings,
            workflow="test_taskfile",
            algorithm="longest_first",
        )

    def test_confidence_no_samples(self):
        """Zero samples gives zero confidence."""
        result = self.optimizer._calculate_confidence([])
        self.assertEqual(result, 0.0)

    def test_confidence_single_sample(self):
        """Single sample gives partial confidence."""
        result = self.optimizer._calculate_confidence([100.0])
        # quantity_factor = min(1, 1/10) * 0.5 = 0.05
        # consistency_factor = 0.25 (single sample default)
        self.assertAlmostEqual(result, 0.30, places=2)

    def test_confidence_max_samples(self):
        """10+ samples gives max quantity factor (0.5)."""
        samples = [100.0] * 10  # Perfectly consistent
        result = self.optimizer._calculate_confidence(samples)
        # quantity_factor = 0.5
        # consistency_factor = 0.5 (zero variance)
        self.assertAlmostEqual(result, 1.0, places=2)

    def test_confidence_high_variance(self):
        """High variance reduces consistency factor."""
        samples = [10.0, 100.0, 50.0, 200.0, 5.0]  # High variance
        result = self.optimizer._calculate_confidence(samples)
        # Should be less than perfect confidence
        self.assertLess(result, 0.8)


class TestTaskOptimizerSorting(TestCase):
    """Tests for task sorting in TaskOptimizer."""

    def setUp(self):
        """Set up optimizer with mock data."""
        self.stats_db = Mock(spec=StatsDatabase)
        self.stats_db.enabled = True

        self.settings = OptimizationSettings(
            min_samples=2,  # Low threshold for testing
        )

        self.optimizer = TaskOptimizer(
            stats_db=self.stats_db,
            settings=self.settings,
            workflow="test_taskfile",
            algorithm="longest_first",
        )

    def test_sort_tasks_longest_first(self):
        """Tasks are sorted by runtime, longest first."""
        # Create tasks with different estimated durations
        task1 = MockTask("task1", "inst1", "fast", {})
        task2 = MockTask("task2", "inst1", "slow", {})
        task3 = MockTask("task3", "inst1", "medium", {})

        # Pre-populate cache with estimates
        sig1 = calculate_task_signature("inst1", "fast", {})
        sig2 = calculate_task_signature("inst1", "slow", {})
        sig3 = calculate_task_signature("inst1", "medium", {})

        self.optimizer._cache[sig1] = RuntimeEstimate(
            sig1, ewma_duration=10.0, sample_count=5, confidence=0.8, estimated=False
        )
        self.optimizer._cache[sig2] = RuntimeEstimate(
            sig2, ewma_duration=100.0, sample_count=5, confidence=0.8, estimated=False
        )
        self.optimizer._cache[sig3] = RuntimeEstimate(
            sig3, ewma_duration=50.0, sample_count=5, confidence=0.8, estimated=False
        )

        tasks = [task1, task2, task3]
        sorted_tasks = self.optimizer.sort_tasks(tasks)

        # Should be ordered: slow (100), medium (50), fast (10)
        self.assertEqual(sorted_tasks[0].id, "task2")  # slow - 100s
        self.assertEqual(sorted_tasks[1].id, "task3")  # medium - 50s
        self.assertEqual(sorted_tasks[2].id, "task1")  # fast - 10s

    def test_sort_empty_list(self):
        """Sorting empty list returns empty list."""
        result = self.optimizer.sort_tasks([])
        self.assertEqual(result, [])

    def test_sort_single_task(self):
        """Sorting single task returns same task."""
        task = MockTask("task1", "inst1", "proc1", {})
        result = self.optimizer.sort_tasks([task])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "task1")

    def test_sort_uses_default_when_insufficient_samples(self):
        """Tasks with insufficient samples use default estimate."""
        task1 = MockTask("task1", "inst1", "has_history", {})
        task2 = MockTask("task2", "inst1", "no_history", {})

        sig1 = calculate_task_signature("inst1", "has_history", {})
        sig2 = calculate_task_signature("inst1", "no_history", {})

        # Task1 has enough samples, Task2 does not
        self.optimizer._cache[sig1] = RuntimeEstimate(
            sig1, ewma_duration=100.0, sample_count=5, confidence=0.8, estimated=False
        )
        self.optimizer._cache[sig2] = RuntimeEstimate(
            sig2, ewma_duration=200.0, sample_count=1, confidence=0.1, estimated=True
        )
        self.optimizer._default_estimate = 50.0  # Default is lower

        tasks = [task1, task2]
        sorted_tasks = self.optimizer.sort_tasks(tasks)

        # Task1 (100s with history) should come before Task2 (uses default 50s)
        self.assertEqual(sorted_tasks[0].id, "task1")
        self.assertEqual(sorted_tasks[1].id, "task2")

    def test_sort_graceful_degradation_on_error(self):
        """On error, returns original task order."""
        task1 = MockTask("task1", "inst1", "proc1", {})
        task2 = MockTask("task2", "inst1", "proc2", {})

        # Make get_estimate raise an exception
        with patch.object(self.optimizer, "get_estimate", side_effect=Exception("Test error")):
            result = self.optimizer.sort_tasks([task1, task2])

        # Should return original order
        self.assertEqual(result[0].id, "task1")
        self.assertEqual(result[1].id, "task2")


class TestTaskOptimizerAlgorithmDispatch(TestCase):
    """Tests for algorithm dispatch in sort_tasks()."""

    def setUp(self):
        """Set up mock data."""
        self.stats_db = Mock(spec=StatsDatabase)
        self.stats_db.enabled = True

        self.settings = OptimizationSettings(min_samples=2)

        self.task1 = MockTask("task1", "inst1", "fast", {})
        self.task2 = MockTask("task2", "inst1", "slow", {})
        self.task3 = MockTask("task3", "inst1", "medium", {})

        self.sig1 = calculate_task_signature("inst1", "fast", {})
        self.sig2 = calculate_task_signature("inst1", "slow", {})
        self.sig3 = calculate_task_signature("inst1", "medium", {})

    def _populate_cache(self, optimizer):
        """Helper to populate cache with known estimates."""
        optimizer._cache[self.sig1] = RuntimeEstimate(
            self.sig1, ewma_duration=10.0, sample_count=5, confidence=0.8, estimated=False
        )
        optimizer._cache[self.sig2] = RuntimeEstimate(
            self.sig2, ewma_duration=100.0, sample_count=5, confidence=0.8, estimated=False
        )
        optimizer._cache[self.sig3] = RuntimeEstimate(
            self.sig3, ewma_duration=50.0, sample_count=5, confidence=0.8, estimated=False
        )

    def test_sort_tasks_longest_first(self):
        """longest_first sorts tasks by runtime descending."""
        optimizer = TaskOptimizer(
            stats_db=self.stats_db,
            settings=self.settings,
            workflow="test",
            algorithm="longest_first",
        )
        self._populate_cache(optimizer)

        tasks = [self.task1, self.task2, self.task3]
        sorted_tasks = optimizer.sort_tasks(tasks)

        # Descending: slow (100), medium (50), fast (10)
        self.assertEqual(sorted_tasks[0].id, "task2")
        self.assertEqual(sorted_tasks[1].id, "task3")
        self.assertEqual(sorted_tasks[2].id, "task1")

    def test_sort_tasks_shortest_first(self):
        """shortest_first sorts tasks by runtime ascending."""
        optimizer = TaskOptimizer(
            stats_db=self.stats_db,
            settings=self.settings,
            workflow="test",
            algorithm="shortest_first",
        )
        self._populate_cache(optimizer)

        tasks = [self.task1, self.task2, self.task3]
        sorted_tasks = optimizer.sort_tasks(tasks)

        # Ascending: fast (10), medium (50), slow (100)
        self.assertEqual(sorted_tasks[0].id, "task1")
        self.assertEqual(sorted_tasks[1].id, "task3")
        self.assertEqual(sorted_tasks[2].id, "task2")

    def test_sort_tasks_empty_list(self):
        """Both algorithms handle empty list."""
        for algo in VALID_ALGORITHMS:
            optimizer = TaskOptimizer(
                stats_db=self.stats_db,
                settings=self.settings,
                workflow="test",
                algorithm=algo,
            )
            result = optimizer.sort_tasks([])
            self.assertEqual(result, [])

    def test_sort_tasks_single_task(self):
        """Both algorithms handle single task."""
        task = MockTask("task1", "inst1", "proc1", {})
        for algo in VALID_ALGORITHMS:
            optimizer = TaskOptimizer(
                stats_db=self.stats_db,
                settings=self.settings,
                workflow="test",
                algorithm=algo,
            )
            result = optimizer.sort_tasks([task])
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].id, "task1")

    def test_invalid_algorithm_raises(self):
        """Invalid algorithm name raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            TaskOptimizer(
                stats_db=self.stats_db,
                settings=self.settings,
                workflow="test",
                algorithm="invalid_algo",
            )
        self.assertIn("Invalid optimization algorithm", str(ctx.exception))


class TestTaskOptimizerCaching(TestCase):
    """Tests for cache behavior in TaskOptimizer."""

    def setUp(self):
        """Set up optimizer."""
        self.stats_db = Mock(spec=StatsDatabase)
        self.stats_db.enabled = True
        self.stats_db.get_task_history.return_value = []

        self.settings = OptimizationSettings(
            time_of_day_weighting=False,
            cache_duration_hours=24,
        )

        self.optimizer = TaskOptimizer(
            stats_db=self.stats_db,
            settings=self.settings,
            workflow="test_taskfile",
            algorithm="longest_first",
        )

    def test_cache_initially_invalid(self):
        """Cache is invalid before build_cache is called."""
        self.assertFalse(self.optimizer.is_cache_valid())

    def test_cache_valid_after_build(self):
        """Cache is valid after build_cache is called."""
        self.optimizer.build_cache([])
        self.assertTrue(self.optimizer.is_cache_valid())

    def test_cache_disabled_with_time_of_day_weighting(self):
        """Cache is always invalid when time_of_day_weighting is enabled."""
        self.optimizer.settings.time_of_day_weighting = True
        self.optimizer.build_cache([])
        # Should still be invalid because time_of_day_weighting disables caching
        self.assertFalse(self.optimizer.is_cache_valid())

    def test_build_cache_populates_estimates(self):
        """build_cache populates the cache with estimates."""
        task = MockTask("task1", "inst1", "proc1", {"p1": "v1"})

        # Mock historical data
        self.stats_db.get_task_history.return_value = [
            {"duration_seconds": 100.0},
            {"duration_seconds": 110.0},
            {"duration_seconds": 90.0},
        ]

        self.optimizer.build_cache([task])

        # Cache should have an entry for this task's signature
        sig = calculate_task_signature("inst1", "proc1", {"p1": "v1"})
        self.assertIn(sig, self.optimizer._cache)
        self.assertFalse(self.optimizer._cache[sig].estimated)


class TestCreateTaskOptimizer(TestCase):
    """Tests for create_task_optimizer factory function."""

    def test_returns_none_when_stats_disabled(self):
        """Returns None when stats database is disabled."""
        stats_db = Mock(spec=StatsDatabase)
        stats_db.enabled = False

        settings = OptimizationSettings()

        result = create_task_optimizer(stats_db, settings, "test_taskfile", "longest_first")
        self.assertIsNone(result)

    def test_returns_none_when_stats_none(self):
        """Returns None when stats database is None."""
        settings = OptimizationSettings()

        result = create_task_optimizer(None, settings, "test_taskfile", "longest_first")
        self.assertIsNone(result)

    def test_returns_optimizer_when_stats_enabled(self):
        """Returns TaskOptimizer when stats are enabled."""
        stats_db = Mock(spec=StatsDatabase)
        stats_db.enabled = True

        settings = OptimizationSettings()

        result = create_task_optimizer(stats_db, settings, "test_taskfile", "longest_first")
        self.assertIsNotNone(result)
        self.assertIsInstance(result, TaskOptimizer)
        self.assertEqual(result.algorithm, "longest_first")

    def test_returns_optimizer_with_shortest_first(self):
        """Returns TaskOptimizer with shortest_first algorithm."""
        stats_db = Mock(spec=StatsDatabase)
        stats_db.enabled = True

        settings = OptimizationSettings()

        result = create_task_optimizer(stats_db, settings, "test_taskfile", "shortest_first")
        self.assertIsNotNone(result)
        self.assertEqual(result.algorithm, "shortest_first")


class TestDefaultEstimateCalculation(TestCase):
    """Tests for default estimate calculation."""

    def setUp(self):
        """Set up optimizer."""
        self.stats_db = Mock(spec=StatsDatabase)
        self.stats_db.enabled = True

        self.settings = OptimizationSettings()

        self.optimizer = TaskOptimizer(
            stats_db=self.stats_db,
            settings=self.settings,
            workflow="test_taskfile",
            algorithm="longest_first",
        )

    def test_default_estimate_uses_fastest_25_percent(self):
        """Default estimate is average of fastest 25% of tasks."""
        estimates = [
            RuntimeEstimate("s1", 10.0, 5, 0.8, False),
            RuntimeEstimate("s2", 20.0, 5, 0.8, False),
            RuntimeEstimate("s3", 30.0, 5, 0.8, False),
            RuntimeEstimate("s4", 100.0, 5, 0.8, False),
        ]

        result = self.optimizer._calculate_default_estimate(estimates)

        # Fastest 25% (1 task) is 10.0
        self.assertAlmostEqual(result, 10.0, places=1)

    def test_default_estimate_empty_list(self):
        """Default estimate with empty list returns fallback."""
        result = self.optimizer._calculate_default_estimate([])
        self.assertEqual(result, 10.0)  # Fallback value

    def test_default_estimate_single_task(self):
        """Default estimate with single task uses that task's duration."""
        estimates = [
            RuntimeEstimate("s1", 50.0, 5, 0.8, False),
        ]

        result = self.optimizer._calculate_default_estimate(estimates)
        self.assertAlmostEqual(result, 50.0, places=1)
