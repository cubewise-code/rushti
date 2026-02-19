"""Contention-aware optimization for RushTI task scheduling.

Analyzes historical execution data to detect parameter-driven resource contention
and generates optimized taskfiles with predecessor chains that prevent heavy tasks
from running concurrently.

The algorithm:
1. Computes EWMA duration per task signature from historical runs
2. Identifies which task parameter drives duration variance (the "contention driver")
3. Groups tasks by contention-driver value
4. Detects heavy outlier groups using IQR-based statistics
5. Builds predecessor chains so heavy groups run sequentially, not concurrently
6. Recommends an optimal max_workers value
7. Writes an optimized taskfile with chains and reordered tasks
"""

import json
import logging
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from rushti.stats import StatsDatabase

logger = logging.getLogger(__name__)


@dataclass
class ParameterAnalysis:
    """Analysis of a single parameter key's impact on task duration."""

    key: str
    distinct_values: int
    group_averages: Dict[str, float]  # {param_value: avg_ewma_duration}
    range_seconds: float  # max_group_avg - min_group_avg


@dataclass
class ContentionGroup:
    """A group of tasks sharing the same contention-driver value."""

    driver_value: str
    task_ids: List[str]
    avg_duration: float
    is_heavy: bool = False


@dataclass
class ContentionAnalysisResult:
    """Complete result of contention analysis."""

    contention_driver: Optional[str]  # parameter key that drives contention
    fan_out_keys: List[str]  # remaining varying parameter keys
    heavy_groups: List[ContentionGroup]  # outlier groups (sorted heaviest first)
    light_groups: List[ContentionGroup]  # non-outlier groups
    all_groups: List[ContentionGroup]  # all groups sorted by duration desc
    chain_length: int  # number of heavy groups in each chain
    fan_out_size: int  # number of independent chains
    critical_path_seconds: float  # sum of heavy group durations
    recommended_workers: int
    sensitivity: float  # IQR multiplier used
    iqr_stats: Dict[str, float]  # q1, q3, iqr, upper_fence
    predecessor_map: Dict[str, List[str]]  # {task_id: [predecessor_ids]}
    warnings: List[str] = field(default_factory=list)
    parameter_analyses: List["ParameterAnalysis"] = field(default_factory=list)

    @property
    def total_tasks(self) -> int:
        return sum(len(g.task_ids) for g in self.all_groups)

    @property
    def heavy_task_count(self) -> int:
        return sum(len(g.task_ids) for g in self.heavy_groups)

    @property
    def light_task_count(self) -> int:
        return sum(len(g.task_ids) for g in self.light_groups)


def _compute_ewma_durations(
    stats_db: StatsDatabase,
    workflow: str,
    lookback_runs: int = 10,
    alpha: float = 0.3,
) -> Dict[str, float]:
    """Compute EWMA duration for every task signature in the workflow.

    Reuses the same EWMA algorithm as TaskOptimizer._calculate_ewma().

    :param stats_db: Stats database connection
    :param workflow: Workflow name
    :param lookback_runs: Number of recent runs to consider
    :param alpha: EWMA smoothing factor
    :return: {task_signature: ewma_duration}
    """
    signatures = stats_db.get_workflow_signatures(workflow)
    if not signatures:
        return {}

    ewma_map = {}
    for sig in signatures:
        durations = stats_db.get_task_durations(sig, limit=lookback_runs)
        if not durations:
            continue

        # EWMA with outlier dampening (same as optimizer.py)
        ewma = durations[0]
        for d in durations[1:]:
            if ewma > 0 and d > ewma * 3.0:
                d = min(d, ewma * 2.0)
            ewma = alpha * d + (1 - alpha) * ewma

        ewma_map[sig] = ewma

    return ewma_map


def _get_task_parameters(
    stats_db: StatsDatabase,
    workflow: str,
) -> List[Dict[str, Any]]:
    """Get task_id, task_signature, process, and parameters for the most recent run.

    :param stats_db: Stats database connection
    :param workflow: Workflow name
    :return: List of dicts with task_id, task_signature, process, parameters (parsed)
    """
    cursor = stats_db._conn.cursor()
    cursor.execute(
        """
        SELECT task_id, task_signature, process, parameters
        FROM task_results
        WHERE run_id = (
            SELECT run_id FROM runs
            WHERE workflow = ? AND status = 'Success'
            ORDER BY start_time DESC LIMIT 1
        )
        ORDER BY CAST(task_id AS INTEGER)
        """,
        (workflow,),
    )

    results = []
    for row in cursor.fetchall():
        params = json.loads(row["parameters"]) if row["parameters"] else {}
        results.append(
            {
                "task_id": row["task_id"],
                "task_signature": row["task_signature"],
                "process": row["process"],
                "parameters": params,
            }
        )
    return results


def _identify_varying_parameters(
    task_params: List[Dict[str, Any]],
) -> List[str]:
    """Identify parameter keys that vary across tasks.

    Filters out constants (keys where all tasks have the same value).

    :param task_params: List of task dicts with 'parameters' field
    :return: List of varying parameter key names
    """
    if not task_params:
        return []

    # Collect all values per key
    key_values: Dict[str, set] = {}
    for task in task_params:
        for key, value in task["parameters"].items():
            if key not in key_values:
                key_values[key] = set()
            key_values[key].add(str(value))

    # Keep only keys with >1 distinct value
    return [key for key, values in key_values.items() if len(values) > 1]


def _find_contention_driver(
    task_params: List[Dict[str, Any]],
    ewma_map: Dict[str, float],
    varying_keys: List[str],
    min_range_ratio: float = 5.0,
) -> Tuple[Optional[ParameterAnalysis], List[ParameterAnalysis]]:
    """Find the parameter that most strongly drives duration variance.

    For each varying parameter, groups tasks by that parameter's value and
    computes the range of group average durations. The parameter with the
    largest range is the contention driver.

    :param task_params: Task parameter data
    :param ewma_map: {signature: ewma_duration}
    :param varying_keys: Parameter keys that vary across tasks
    :param min_range_ratio: Minimum ratio of winner to runner-up range
    :return: (winner ParameterAnalysis or None, all analyses sorted by range desc)
    """
    if not varying_keys:
        return None, []

    analyses = []
    for key in varying_keys:
        # Group tasks by this parameter's value
        groups: Dict[str, List[float]] = {}
        for task in task_params:
            value = str(task["parameters"].get(key, ""))
            sig = task["task_signature"]
            if sig in ewma_map:
                if value not in groups:
                    groups[value] = []
                groups[value].append(ewma_map[sig])

        # Compute average per group
        group_avgs = {}
        for value, durations in groups.items():
            if durations:
                group_avgs[value] = sum(durations) / len(durations)

        if not group_avgs:
            continue

        range_seconds = max(group_avgs.values()) - min(group_avgs.values())

        analyses.append(
            ParameterAnalysis(
                key=key,
                distinct_values=len(group_avgs),
                group_averages=group_avgs,
                range_seconds=range_seconds,
            )
        )

    # Sort by range descending
    analyses.sort(key=lambda a: a.range_seconds, reverse=True)

    if not analyses:
        return None, analyses

    winner = analyses[0]

    # Validate: winner must have significantly larger range than runner-up
    if len(analyses) > 1:
        runner_up = analyses[1]
        if runner_up.range_seconds > 0:
            ratio = winner.range_seconds / runner_up.range_seconds
            if ratio < min_range_ratio:
                logger.warning(
                    f"Ambiguous contention signal: {winner.key} range={winner.range_seconds:.1f}s "
                    f"vs {runner_up.key} range={runner_up.range_seconds:.1f}s (ratio={ratio:.1f}x, "
                    f"need {min_range_ratio}x). Contention driver unclear."
                )
                return None, analyses

    return winner, analyses


def _detect_heavy_outliers(
    groups: List[ContentionGroup],
    sensitivity: float = 10.0,
) -> Tuple[List[ContentionGroup], List[ContentionGroup], Dict[str, float]]:
    """Detect heavy outlier groups using IQR-based statistics.

    :param groups: All contention groups with avg_duration set
    :param sensitivity: IQR multiplier (higher = more conservative)
    :return: (heavy_groups, light_groups, iqr_stats_dict)
    """
    if len(groups) < 4:
        # Too few groups for meaningful IQR
        return [], groups, {"q1": 0, "q3": 0, "iqr": 0, "upper_fence": 0}

    durations = sorted(g.avg_duration for g in groups)
    n = len(durations)

    # Calculate quartiles
    q1_idx = n // 4
    q3_idx = (3 * n) // 4
    q1 = durations[q1_idx]
    q3 = durations[q3_idx]
    iqr = q3 - q1
    upper_fence = q3 + sensitivity * iqr

    iqr_stats = {
        "q1": round(q1, 2),
        "q3": round(q3, 2),
        "iqr": round(iqr, 2),
        "upper_fence": round(upper_fence, 2),
    }

    heavy = []
    light = []
    for g in groups:
        if g.avg_duration > upper_fence:
            g.is_heavy = True
            heavy.append(g)
        else:
            light.append(g)

    # Sort heavy by duration descending (heaviest first for chaining)
    heavy.sort(key=lambda g: g.avg_duration, reverse=True)
    light.sort(key=lambda g: g.avg_duration, reverse=True)

    return heavy, light, iqr_stats


def _build_predecessor_chains(
    heavy_groups: List[ContentionGroup],
    task_params: List[Dict[str, Any]],
    contention_driver: str,
    fan_out_keys: List[str],
) -> Dict[str, List[str]]:
    """Build predecessor chains for heavy groups across fan-out dimensions.

    For each unique fan-out value, creates a chain through heavy groups:
    heaviest → second heaviest → ... → lightest heavy group.

    :param heavy_groups: Heavy groups sorted by duration descending
    :param task_params: All task parameter data
    :param contention_driver: The contention-driving parameter key
    :param fan_out_keys: The fan-out parameter keys
    :return: {task_id: [predecessor_task_id]}
    """
    if len(heavy_groups) < 2:
        return {}

    heavy_driver_values = [g.driver_value for g in heavy_groups]

    # Build lookup: (driver_value, fan_out_value_tuple) → task_id
    task_lookup: Dict[Tuple[str, str], str] = {}
    for task in task_params:
        driver_val = str(task["parameters"].get(contention_driver, ""))
        # Create a composite fan-out key from all fan-out parameters
        fan_out_val = "|".join(str(task["parameters"].get(k, "")) for k in sorted(fan_out_keys))
        task_lookup[(driver_val, fan_out_val)] = task["task_id"]

    # Get all unique fan-out values
    fan_out_values = set()
    for task in task_params:
        fan_out_val = "|".join(str(task["parameters"].get(k, "")) for k in sorted(fan_out_keys))
        fan_out_values.add(fan_out_val)

    # Build chains: for each fan-out value, chain heavy groups sequentially
    predecessor_map: Dict[str, List[str]] = {}
    for fan_out_val in fan_out_values:
        for i in range(1, len(heavy_driver_values)):
            current_driver = heavy_driver_values[i]
            pred_driver = heavy_driver_values[i - 1]

            current_task_id = task_lookup.get((current_driver, fan_out_val))
            pred_task_id = task_lookup.get((pred_driver, fan_out_val))

            if current_task_id and pred_task_id:
                predecessor_map[current_task_id] = [pred_task_id]

    return predecessor_map


def _recommend_max_workers(
    heavy_groups: List[ContentionGroup],
    light_groups: List[ContentionGroup],
    fan_out_size: int,
) -> int:
    """Recommend optimal max_workers based on chain structure.

    Formula: chain_slots + ceil(light_work / chain_duration)

    :param heavy_groups: Heavy groups (for critical path calculation)
    :param light_groups: Light groups (for light work calculation)
    :param fan_out_size: Number of independent chains
    :return: Recommended max_workers value
    """
    chain_slots = fan_out_size

    # Critical path = sum of heavy group durations (one chain)
    critical_path = sum(g.avg_duration for g in heavy_groups) if heavy_groups else 1.0

    # Total light work = number of light tasks × average light duration
    light_durations = []
    for g in light_groups:
        light_durations.extend([g.avg_duration] * len(g.task_ids))
    light_total_work = sum(light_durations) if light_durations else 0.0

    # Workers needed for light tasks to finish within the chain window
    light_slots = math.ceil(light_total_work / critical_path) if critical_path > 0 else 0

    recommended = chain_slots + light_slots

    # Floor at fan_out_size (at minimum, need enough for all chains)
    # Ceiling at a reasonable limit (diminishing returns beyond 2x the formula)
    return max(fan_out_size, recommended)


def analyze_contention(
    stats_db: StatsDatabase,
    workflow: str,
    task_params: Optional[List[Dict[str, Any]]] = None,
    sensitivity: float = 10.0,
    lookback_runs: int = 10,
    ewma_alpha: float = 0.3,
    min_range_ratio: float = 5.0,
) -> ContentionAnalysisResult:
    """Run the full contention-aware analysis on a workflow.

    This is the main entry point for the contention_aware algorithm.

    :param stats_db: Stats database connection
    :param workflow: Workflow name
    :param task_params: Task parameters (if None, fetched from most recent run)
    :param sensitivity: IQR multiplier for outlier detection (higher = more conservative)
    :param lookback_runs: Number of historical runs for EWMA
    :param ewma_alpha: EWMA smoothing factor
    :param min_range_ratio: Minimum ratio for contention driver signal clarity
    :return: ContentionAnalysisResult
    """
    warnings: List[str] = []

    # Step 1: Compute EWMA durations
    ewma_map = _compute_ewma_durations(stats_db, workflow, lookback_runs, ewma_alpha)
    if not ewma_map:
        return _empty_result("No historical data found for workflow", sensitivity)

    # Get task parameters from most recent run
    if task_params is None:
        task_params = _get_task_parameters(stats_db, workflow)
    if not task_params:
        return _empty_result("No task data found for workflow", sensitivity)

    # Step 2: Identify varying parameters
    varying_keys = _identify_varying_parameters(task_params)
    if not varying_keys:
        return _empty_result(
            "No varying parameters found — all tasks have identical parameters", sensitivity
        )

    logger.info(f"Varying parameters: {varying_keys}")

    # Step 3: Find contention driver
    driver_analysis, all_analyses = _find_contention_driver(
        task_params, ewma_map, varying_keys, min_range_ratio
    )

    if driver_analysis is None:
        msg = "Could not identify a clear contention-driving parameter"
        if all_analyses:
            details = ", ".join(f"{a.key}={a.range_seconds:.1f}s" for a in all_analyses[:3])
            msg += f" (ranges: {details})"
        return _empty_result(msg, sensitivity)

    contention_driver = driver_analysis.key
    fan_out_keys = [k for k in varying_keys if k != contention_driver]

    logger.info(
        f"Contention driver: {contention_driver} (range={driver_analysis.range_seconds:.1f}s), "
        f"fan-out: {fan_out_keys}"
    )

    # Step 4: Determine fan-out size
    fan_out_values = set()
    for task in task_params:
        fan_out_val = "|".join(str(task["parameters"].get(k, "")) for k in sorted(fan_out_keys))
        fan_out_values.add(fan_out_val)
    fan_out_size = len(fan_out_values) if fan_out_values else 1

    # Step 5: Group tasks by contention driver
    groups_dict: Dict[str, ContentionGroup] = {}
    for task in task_params:
        driver_val = str(task["parameters"].get(contention_driver, ""))
        if driver_val not in groups_dict:
            groups_dict[driver_val] = ContentionGroup(
                driver_value=driver_val,
                task_ids=[],
                avg_duration=0.0,
            )
        groups_dict[driver_val].task_ids.append(task["task_id"])

    # Set group average duration from driver analysis
    for driver_val, group in groups_dict.items():
        group.avg_duration = driver_analysis.group_averages.get(driver_val, 0.0)

    all_groups = sorted(groups_dict.values(), key=lambda g: g.avg_duration, reverse=True)

    # Step 6: Detect heavy outliers
    heavy_groups, light_groups, iqr_stats = _detect_heavy_outliers(all_groups, sensitivity)

    if len(heavy_groups) < 2:
        msg = f"Only {len(heavy_groups)} heavy group(s) detected — chaining requires at least 2"
        warnings.append(msg)
        logger.info(msg)
        # Still return the analysis, just with empty chains
        return ContentionAnalysisResult(
            contention_driver=contention_driver,
            fan_out_keys=fan_out_keys,
            heavy_groups=heavy_groups,
            light_groups=light_groups if heavy_groups else all_groups,
            all_groups=all_groups,
            chain_length=len(heavy_groups),
            fan_out_size=fan_out_size,
            critical_path_seconds=sum(g.avg_duration for g in heavy_groups),
            recommended_workers=fan_out_size,
            sensitivity=sensitivity,
            iqr_stats=iqr_stats,
            predecessor_map={},
            warnings=warnings,
            parameter_analyses=all_analyses,
        )

    logger.info(
        f"Detected {len(heavy_groups)} heavy groups (above {iqr_stats['upper_fence']:.1f}s): "
        + ", ".join(f"{g.driver_value}({g.avg_duration:.1f}s)" for g in heavy_groups)
    )

    # Step 7: Build predecessor chains
    predecessor_map = _build_predecessor_chains(
        heavy_groups, task_params, contention_driver, fan_out_keys
    )

    # Step 8: Recommend max_workers
    recommended_workers = _recommend_max_workers(heavy_groups, light_groups, fan_out_size)

    critical_path = sum(g.avg_duration for g in heavy_groups)

    return ContentionAnalysisResult(
        contention_driver=contention_driver,
        fan_out_keys=fan_out_keys,
        heavy_groups=heavy_groups,
        light_groups=light_groups,
        all_groups=all_groups,
        chain_length=len(heavy_groups),
        fan_out_size=fan_out_size,
        critical_path_seconds=critical_path,
        recommended_workers=recommended_workers,
        sensitivity=sensitivity,
        iqr_stats=iqr_stats,
        predecessor_map=predecessor_map,
        warnings=warnings,
        parameter_analyses=all_analyses,
    )


def get_archived_taskfile_path(
    stats_db: StatsDatabase,
    workflow: str,
) -> Optional[str]:
    """Get the archived taskfile path from the most recent successful run.

    The archive stores a JSON snapshot of every run's taskfile at
    ``archive/{workflow}/{run_id}.json``, and the path is recorded in the
    ``runs.taskfile_path`` column.

    :param stats_db: Stats database connection
    :param workflow: Workflow name
    :return: Path to archived JSON taskfile, or None if no successful runs exist
    """
    cursor = stats_db._conn.cursor()
    cursor.execute(
        """
        SELECT taskfile_path FROM runs
        WHERE workflow = ? AND status = 'Success'
        ORDER BY start_time DESC LIMIT 1
        """,
        (workflow,),
    )
    row = cursor.fetchone()
    if row and row["taskfile_path"]:
        return row["taskfile_path"]
    return None


def write_optimized_taskfile(
    original_taskfile_path: str,
    result: ContentionAnalysisResult,
    output_path: str,
) -> None:
    """Write an optimized taskfile with predecessor chains and reordered tasks.

    Reorders tasks to contention-driver-major (groups by driver value, then fan-out
    within each group). Injects predecessor chains for heavy groups. Embeds the
    recommended ``max_workers`` in the taskfile settings so it takes effect
    automatically (the CLI ``--max-workers`` flag still overrides).

    :param original_taskfile_path: Path to original JSON taskfile
    :param result: Contention analysis result
    :param output_path: Output path for optimized taskfile
    """
    from rushti.taskfile import parse_json_taskfile

    taskfile = parse_json_taskfile(original_taskfile_path)

    # Apply predecessors from analysis
    task_by_id = {t.id: t for t in taskfile.tasks}
    for task_id, pred_ids in result.predecessor_map.items():
        if task_id in task_by_id:
            task_by_id[task_id].predecessors = pred_ids

    # Reorder tasks: contention-driver-major
    if result.contention_driver:
        driver_key = result.contention_driver

        # Order: heavy groups first (heaviest first), then light groups
        group_order = [g.driver_value for g in result.heavy_groups] + [
            g.driver_value for g in result.light_groups
        ]

        # Build ordered task list
        tasks_by_driver: Dict[str, List] = {}
        for task in taskfile.tasks:
            driver_val = str(task.parameters.get(driver_key, ""))
            if driver_val not in tasks_by_driver:
                tasks_by_driver[driver_val] = []
            tasks_by_driver[driver_val].append(task)

        reordered = []
        for driver_val in group_order:
            if driver_val in tasks_by_driver:
                reordered.extend(tasks_by_driver[driver_val])

        # Add any tasks not covered (safety net)
        covered_ids = {t.id for t in reordered}
        for task in taskfile.tasks:
            if task.id not in covered_ids:
                reordered.append(task)

        taskfile.tasks = reordered

    # Embed recommended max_workers in taskfile settings
    if result.recommended_workers > 0:
        taskfile.settings.max_workers = result.recommended_workers

    # Update metadata
    heavy_vals = [g.driver_value for g in result.heavy_groups]
    chain_desc = ">".join(heavy_vals) if heavy_vals else "none"
    taskfile.metadata.description = (
        f"Contention-aware optimized: driver={result.contention_driver}, "
        f"chain=[{chain_desc}], sensitivity={result.sensitivity}, "
        f"recommended_workers={result.recommended_workers}"
    )

    # Write output
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    taskfile.save(output)

    logger.info(f"Optimized taskfile written to: {output}")


def _empty_result(warning: str, sensitivity: float) -> ContentionAnalysisResult:
    """Create an empty result with a warning message."""
    logger.warning(warning)
    return ContentionAnalysisResult(
        contention_driver=None,
        fan_out_keys=[],
        heavy_groups=[],
        light_groups=[],
        all_groups=[],
        chain_length=0,
        fan_out_size=0,
        critical_path_seconds=0.0,
        recommended_workers=0,
        sensitivity=sensitivity,
        iqr_stats={},
        predecessor_map={},
        warnings=[warning],
    )
