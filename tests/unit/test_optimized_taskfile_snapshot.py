"""Golden-file snapshots for the two optimized-taskfile writers.

Pins the current output of:

- ``rushti.taskfile_ops.write_ewma_optimized_taskfile`` (EWMA-based
  reordering of task IDs in place)
- ``rushti.contention_analyzer.write_contention_optimized_taskfile``
  (contention-driver reordering with predecessor-chain injection and
  ``max_workers`` embedding)

The two functions solve different problems and live in different
modules; this file captures both outputs into goldens at
``tests/resources/golden/`` so any behavior drift surfaces as a test
failure with a unified diff.

To regenerate goldens after an intentional behavior change:

    RUSHTI_REGENERATE_GOLDENS=1 pytest tests/unit/test_optimized_taskfile_snapshot.py
"""

import json
import unittest

from rushti.contention_analyzer import (
    ContentionAnalysisResult,
    ContentionGroup,
)
from rushti.contention_analyzer import (
    write_contention_optimized_taskfile as write_contention_optimized,
)
from rushti.taskfile_ops import (
    AnalysisReport,
    TaskAnalysis,
)
from rushti.taskfile_ops import write_ewma_optimized_taskfile as write_ewma_optimized

# Deterministic input taskfile shared by both snapshots.
INPUT_TASKFILE = {
    "version": "2.0",
    "metadata": {
        "workflow": "snapshot-fixture",
        "description": "snapshot test input fixture",
    },
    "settings": {"max_workers": 4},
    "tasks": [
        {
            "id": "task_a",
            "instance": "tm1srv01",
            "process": "}bedrock.cube.data.copy",
            "parameters": {"pCube": "Sales", "pYear": "2024"},
        },
        {
            "id": "task_b",
            "instance": "tm1srv01",
            "process": "}bedrock.cube.data.copy",
            "parameters": {"pCube": "Sales", "pYear": "2025"},
        },
        {
            "id": "task_c",
            "instance": "tm1srv01",
            "process": "}bedrock.cube.data.copy",
            "parameters": {"pCube": "Inventory", "pYear": "2024"},
        },
        {
            "id": "task_d",
            "instance": "tm1srv01",
            "process": "}bedrock.cube.data.copy",
            "parameters": {"pCube": "Inventory", "pYear": "2025"},
        },
        {
            "id": "task_e",
            "instance": "tm1srv01",
            "process": "}bedrock.cube.data.copy",
            "parameters": {"pCube": "Forecast", "pYear": "2025"},
        },
    ],
}


def _write_input(tmp_path, name="input_taskfile.json"):
    path = tmp_path / name
    path.write_text(json.dumps(INPUT_TASKFILE, indent=2))
    return str(path)


class TestEwmaOptimizedSnapshot:
    """Snapshot of taskfile_ops.write_ewma_optimized_taskfile (EWMA-based)."""

    def test_ewma_optimized_taskfile_golden(self, tmp_path, golden_file):
        """Pin the current EWMA-based optimized taskfile output."""
        input_path = _write_input(tmp_path)
        output_path = str(tmp_path / "optimized_ewma.json")

        # Deterministic AnalysisReport — pinned date so the output is stable
        report = AnalysisReport(
            workflow="snapshot-fixture",
            analysis_date="2026-04-30T00:00:00Z",
            run_count=5,
            tasks=[
                TaskAnalysis(
                    task_id="task_a",
                    avg_duration=10.0,
                    ewma_duration=9.5,
                    run_count=5,
                    success_rate=1.0,
                    confidence=0.9,
                    estimated=False,
                ),
                TaskAnalysis(
                    task_id="task_b",
                    avg_duration=20.0,
                    ewma_duration=18.0,
                    run_count=5,
                    success_rate=1.0,
                    confidence=0.9,
                    estimated=False,
                ),
                TaskAnalysis(
                    task_id="task_c",
                    avg_duration=5.0,
                    ewma_duration=4.5,
                    run_count=5,
                    success_rate=1.0,
                    confidence=0.9,
                    estimated=False,
                ),
                TaskAnalysis(
                    task_id="task_d",
                    avg_duration=15.0,
                    ewma_duration=14.0,
                    run_count=5,
                    success_rate=1.0,
                    confidence=0.9,
                    estimated=False,
                ),
                TaskAnalysis(
                    task_id="task_e",
                    avg_duration=8.0,
                    ewma_duration=7.5,
                    run_count=5,
                    success_rate=1.0,
                    confidence=0.9,
                    estimated=False,
                ),
            ],
            recommendations=["Consider parallelizing task_b — longest runtime"],
            optimized_order=["task_b", "task_d", "task_a", "task_e", "task_c"],
            ewma_alpha=0.3,
            lookback_runs=10,
        )

        write_ewma_optimized(input_path, report.optimized_order, output_path, report)

        # Read output, normalize the original_taskfile path so the golden is
        # location-independent.
        with open(output_path) as f:
            output = json.load(f)
        if "metadata" in output and "original_taskfile" in output["metadata"]:
            output["metadata"]["original_taskfile"] = "<INPUT_PATH>"
        actual = json.dumps(output, indent=2, sort_keys=True)

        golden_file(actual, "ewma_optimized_taskfile.json")


class TestContentionOptimizedSnapshot:
    """Snapshot of contention_analyzer.write_contention_optimized_taskfile (contention-aware)."""

    def test_contention_optimized_taskfile_golden(self, tmp_path, golden_file):
        """Pin the current contention-aware optimized taskfile output."""
        input_path = _write_input(tmp_path)
        output_path = str(tmp_path / "optimized_contention.json")

        # Deterministic ContentionAnalysisResult.
        # Driver is "pCube"; "Sales" is the heavy group; the rest are light.
        sales_group = ContentionGroup(
            driver_value="Sales",
            task_ids=["task_a", "task_b"],
            avg_duration=18.0,
            is_heavy=True,
        )
        inventory_group = ContentionGroup(
            driver_value="Inventory",
            task_ids=["task_c", "task_d"],
            avg_duration=10.0,
            is_heavy=False,
        )
        forecast_group = ContentionGroup(
            driver_value="Forecast",
            task_ids=["task_e"],
            avg_duration=7.5,
            is_heavy=False,
        )

        result = ContentionAnalysisResult(
            contention_driver="pCube",
            fan_out_keys=["pYear"],
            heavy_groups=[sales_group],
            light_groups=[inventory_group, forecast_group],
            all_groups=[sales_group, inventory_group, forecast_group],
            chain_length=1,
            fan_out_size=2,
            critical_path_seconds=18.0,
            recommended_workers=2,
            sensitivity=1.5,
            iqr_stats={
                "q1": 7.5,
                "q3": 14.0,
                "iqr": 6.5,
                "upper_fence": 23.75,
            },
            predecessor_map={"task_b": ["task_a"]},
            warnings=[],
            parameter_analyses=[],
        )

        write_contention_optimized(input_path, result, output_path)

        with open(output_path) as f:
            output = json.load(f)
        actual = json.dumps(output, indent=2, sort_keys=True)

        golden_file(actual, "contention_optimized_taskfile.json")


if __name__ == "__main__":
    unittest.main()
