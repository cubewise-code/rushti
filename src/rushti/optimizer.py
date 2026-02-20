"""Task runtime optimization module for RushTI.

Provides runtime estimation and task ordering for optimized parallel execution.
Uses EWMA (Exponentially Weighted Moving Average) based on historical execution data.

Supported scheduling algorithms:
- ``longest_first``: Sort ready tasks by estimated runtime descending (longest first).
  Best for independent workloads where tasks do not contend for shared resources.
- ``shortest_first``: Sort ready tasks by estimated runtime ascending (shortest first).
  Best for shared-resource workloads (e.g. TM1) where running many heavy tasks
  concurrently causes contention and inflated durations.

Dependencies are always preserved — optimization only reorders among ready tasks.
"""

import logging
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from rushti.stats import StatsDatabase, calculate_task_signature

if TYPE_CHECKING:
    from rushti.settings import OptimizationSettings

logger = logging.getLogger(__name__)

# Valid algorithm names
VALID_ALGORITHMS = ("longest_first", "shortest_first")


@dataclass
class RuntimeEstimate:
    """Runtime estimate for a single task."""

    task_signature: str
    ewma_duration: float  # EWMA-estimated duration in seconds
    sample_count: int  # Number of historical samples used
    confidence: float  # Confidence score (0-1)
    estimated: bool  # True if using default estimate (no history)


class TaskOptimizer:
    """Manages task runtime estimation and ordering for DAG execution.

    The optimizer supports multiple scheduling algorithms, selected per-taskfile
    via ``--optimize <algorithm>`` (CLI) or ``"optimization_algorithm"`` (JSON
    taskfile settings).

    Algorithms:
    - ``longest_first`` — Sorts ready tasks by estimated runtime descending.
      Long tasks start early so short tasks can fill remaining worker slots.
      Best for independent workloads with varied task durations.
    - ``shortest_first`` — Sorts ready tasks by estimated runtime ascending.
      Light tasks run first, deferring heavy tasks until workers free up.
      Best for shared-resource TM1 workloads where concurrent heavy tasks
      cause lock contention and inflated durations.

    Workflow:
    1. Builds a cache of EWMA runtime estimates for all tasks at run start
    2. Sorts ready tasks using the configured algorithm during execution
    3. Respects min_samples threshold before applying optimization
    4. Caches results based on cache_duration_hours (unless time_of_day_weighting)

    Attributes:
        stats_db: StatsDatabase for historical data
        settings: OptimizationSettings (EWMA tuning parameters)
        workflow: Identifier for the current workflow
        algorithm: Scheduling algorithm name
    """

    def __init__(
        self,
        stats_db: StatsDatabase,
        settings: "OptimizationSettings",
        workflow: str,
        algorithm: str = "longest_first",
    ):
        """Initialize optimizer with stats database and settings.

        :param stats_db: StatsDatabase instance (must be enabled)
        :param settings: OptimizationSettings from settings.ini (EWMA tuning)
        :param workflow: Identifier for the current workflow
        :param algorithm: Scheduling algorithm — ``longest_first`` or ``shortest_first``
        :raises ValueError: If algorithm is not a valid algorithm name
        """
        if algorithm not in VALID_ALGORITHMS:
            raise ValueError(
                f"Invalid optimization algorithm '{algorithm}'. "
                f"Valid algorithms: {', '.join(VALID_ALGORITHMS)}"
            )

        self.stats_db = stats_db
        self.settings = settings
        self.workflow = workflow
        self.algorithm = algorithm

        # Runtime estimate cache: signature -> RuntimeEstimate
        self._cache: Dict[str, RuntimeEstimate] = {}
        self._cache_time: Optional[datetime] = None
        self._default_estimate: float = 10.0  # Fallback default

        logger.debug(
            f"TaskOptimizer initialized (algorithm={algorithm}, "
            f"lookback={settings.lookback_runs}, "
            f"min_samples={settings.min_samples})"
        )

    def build_cache(self, tasks: List[Any]) -> None:
        """Build runtime estimate cache for all tasks in the DAG.

        Called once at run start. Calculates EWMA for each unique task
        signature and caches results.

        :param tasks: All tasks from the DAG
        """
        if self.is_cache_valid():
            logger.debug("Using cached runtime estimates")
            return

        logger.debug(f"Building runtime estimate cache for {len(tasks)} tasks")

        # Collect unique signatures and their tasks
        signatures: Dict[str, Any] = {}
        for task in tasks:
            signature = calculate_task_signature(
                instance=getattr(task, "instance_name", ""),
                process=getattr(task, "process_name", ""),
                parameters=getattr(task, "parameters", {}),
            )
            if signature not in signatures:
                signatures[signature] = task

        # Calculate estimates for each signature
        estimates_with_history = []
        for signature in signatures:
            estimate = self._calculate_estimate(signature)
            self._cache[signature] = estimate
            if not estimate.estimated:
                estimates_with_history.append(estimate)

        # Calculate default estimate for tasks without history
        if estimates_with_history:
            self._default_estimate = self._calculate_default_estimate(estimates_with_history)

        # Update estimates for tasks that used default
        for signature, estimate in self._cache.items():
            if estimate.estimated:
                self._cache[signature] = RuntimeEstimate(
                    task_signature=signature,
                    ewma_duration=self._default_estimate,
                    sample_count=0,
                    confidence=0.0,
                    estimated=True,
                )

        self._cache_time = datetime.now()

        tasks_with_history = len(estimates_with_history)
        tasks_without = len(signatures) - tasks_with_history
        logger.debug(
            f"Built runtime estimates: {tasks_with_history} with history, "
            f"{tasks_without} using default ({self._default_estimate:.1f}s)"
        )

    def get_estimate(self, task: Any) -> RuntimeEstimate:
        """Get runtime estimate for a single task.

        :param task: Task to get estimate for
        :return: RuntimeEstimate (from cache or default)
        """
        signature = calculate_task_signature(
            instance=getattr(task, "instance_name", ""),
            process=getattr(task, "process_name", ""),
            parameters=getattr(task, "parameters", {}),
        )

        if signature in self._cache:
            return self._cache[signature]

        # Not in cache - calculate on the fly
        estimate = self._calculate_estimate(signature)
        if estimate.estimated:
            estimate = RuntimeEstimate(
                task_signature=signature,
                ewma_duration=self._default_estimate,
                sample_count=0,
                confidence=0.0,
                estimated=True,
            )
        self._cache[signature] = estimate
        return estimate

    def sort_tasks(self, tasks: List[Any]) -> List[Any]:
        """Sort tasks using the configured scheduling algorithm.

        Dispatches to the appropriate sorting strategy based on ``self.algorithm``.
        On any error, returns tasks in original order (graceful degradation).

        :param tasks: List of ready tasks
        :return: Sorted list of tasks
        """
        if not tasks:
            return tasks

        if self.algorithm == "longest_first":
            return self._sort_by_estimated_runtime(tasks, reverse=True)
        elif self.algorithm == "shortest_first":
            return self._sort_by_estimated_runtime(tasks, reverse=False)
        else:
            # Should not happen due to __init__ validation, but be safe
            logger.warning(f"Unknown algorithm '{self.algorithm}', using default order")
            return tasks

    def _sort_by_estimated_runtime(self, tasks: List[Any], reverse: bool = True) -> List[Any]:
        """Sort tasks by estimated runtime.

        :param tasks: List of ready tasks
        :param reverse: True for descending (longest first), False for ascending
                        (shortest first)
        :return: Sorted list of tasks
        """
        try:
            # Get estimates and check min_samples
            task_estimates = []
            for task in tasks:
                estimate = self.get_estimate(task)
                # Only use estimate if we have enough samples
                if estimate.sample_count >= self.settings.min_samples:
                    duration = estimate.ewma_duration
                else:
                    # Not enough samples - use default (will sort to middle/end)
                    duration = self._default_estimate
                task_estimates.append((task, duration))

            # Sort by duration
            task_estimates.sort(key=lambda x: x[1], reverse=reverse)

            sorted_tasks = [t[0] for t in task_estimates]

            if logger.isEnabledFor(logging.DEBUG) and len(sorted_tasks) > 1:
                durations = [f"{te[1]:.1f}s" for te in task_estimates[:5]]
                if len(task_estimates) > 5:
                    durations.append("...")
                order = "desc" if reverse else "asc"
                logger.debug(
                    f"Sorted {len(sorted_tasks)} tasks ({order}): " f"[{', '.join(durations)}]"
                )

            return sorted_tasks

        except Exception as e:
            logger.warning(f"Optimization failed, using default order: {e}")
            return tasks  # Return original order

    def is_cache_valid(self) -> bool:
        """Check if cache is still valid based on cache_duration_hours.

        If time_of_day_weighting is True, always returns False (no caching).

        :return: True if cache is valid and can be reused
        """
        # Time-of-day weighting requires fresh calculations
        if self.settings.time_of_day_weighting:
            return False

        if self._cache_time is None:
            return False

        age_hours = (datetime.now() - self._cache_time).total_seconds() / 3600
        return age_hours < self.settings.cache_duration_hours

    def _calculate_estimate(self, signature: str) -> RuntimeEstimate:
        """Calculate runtime estimate for a task signature.

        :param signature: Task signature hash
        :return: RuntimeEstimate with EWMA calculation
        """
        # Get historical durations
        durations = self._get_task_durations(signature)

        if not durations:
            return RuntimeEstimate(
                task_signature=signature,
                ewma_duration=0.0,
                sample_count=0,
                confidence=0.0,
                estimated=True,
            )

        # Calculate EWMA with outlier dampening
        ewma = self._calculate_ewma(durations)

        # Calculate confidence score
        confidence = self._calculate_confidence(durations)

        return RuntimeEstimate(
            task_signature=signature,
            ewma_duration=ewma,
            sample_count=len(durations),
            confidence=confidence,
            estimated=False,
        )

    def _get_task_durations(self, signature: str) -> List[float]:
        """Get historical durations for a task signature.

        :param signature: Task signature hash
        :return: List of durations (most recent first)
        """
        history = self.stats_db.get_task_history(signature, limit=self.settings.lookback_runs)
        return [h["duration_seconds"] for h in history if h.get("duration_seconds")]

    def _calculate_ewma(self, durations: List[float], alpha: float = 0.3) -> float:
        """Calculate EWMA with outlier dampening.

        Uses the same algorithm as dag.py analyze_runs():
        - Outliers (> 3x current estimate) are capped at 2x
        - Alpha controls smoothing (higher = more weight on recent)

        :param durations: List of durations (most recent first)
        :param alpha: EWMA smoothing factor (0-1)
        :return: EWMA estimate
        """
        if not durations:
            return 0.0

        # Start with most recent value
        ewma = durations[0]

        # Process remaining durations (older first would be traditional,
        # but we process newer first to match analyze_runs behavior)
        for d in durations[1:]:
            # Outlier detection and dampening
            if ewma > 0 and d > ewma * 3.0:
                # Cap at 2x current estimate to prevent spikes
                d_dampened = min(d, ewma * 2.0)
            else:
                d_dampened = d

            # Update EWMA: blend with previous estimate
            ewma = alpha * d_dampened + (1 - alpha) * ewma

        return ewma

    def _calculate_confidence(self, durations: List[float]) -> float:
        """Calculate confidence score for an estimate.

        Confidence is based on:
        - Quantity: More samples = higher confidence (up to 10)
        - Consistency: Lower variance = higher confidence

        :param durations: List of durations
        :return: Confidence score (0-1)
        """
        if not durations:
            return 0.0

        # Quantity factor: 0-0.5 based on sample count (max at 10 samples)
        quantity_factor = min(1.0, len(durations) / 10) * 0.5

        # Consistency factor: 0-0.5 based on coefficient of variation
        if len(durations) >= 2:
            avg = statistics.mean(durations)
            std_dev = statistics.stdev(durations)
            cv = std_dev / avg if avg > 0 else 1.0
            consistency_factor = (1 - min(1.0, cv)) * 0.5
        else:
            consistency_factor = 0.25  # Single sample gets middle confidence

        return quantity_factor + consistency_factor

    def _calculate_default_estimate(self, estimates: List[RuntimeEstimate]) -> float:
        """Calculate default estimate for tasks without history.

        Uses fastest 25% average as per analyze_runs() logic.

        :param estimates: List of estimates with history
        :return: Default estimate value
        """
        if not estimates:
            return 10.0  # Fallback

        # Get all EWMA durations and sort
        durations = sorted([e.ewma_duration for e in estimates if e.ewma_duration > 0])

        if not durations:
            return 10.0

        # Use fastest 25%
        fastest_count = max(1, len(durations) // 4)
        fastest = durations[:fastest_count]

        return statistics.mean(fastest)


def create_task_optimizer(
    stats_db: StatsDatabase,
    settings: "OptimizationSettings",
    workflow: str,
    algorithm: str = "longest_first",
) -> Optional[TaskOptimizer]:
    """Factory function to create TaskOptimizer if conditions are met.

    Returns None if stats_db is None or not enabled.

    :param stats_db: StatsDatabase instance
    :param settings: OptimizationSettings from settings.ini (EWMA tuning)
    :param workflow: Identifier for the current workflow
    :param algorithm: Scheduling algorithm — ``longest_first`` or ``shortest_first``
    :return: TaskOptimizer or None
    """
    if stats_db is None or not stats_db.enabled:
        logger.debug("Cannot create optimizer: stats database not available")
        return None

    return TaskOptimizer(
        stats_db=stats_db,
        settings=settings,
        workflow=workflow,
        algorithm=algorithm,
    )
