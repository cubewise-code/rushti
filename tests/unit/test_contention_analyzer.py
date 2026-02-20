"""Unit tests for the contention_analyzer module."""

import json
import os
import tempfile
from unittest import TestCase
from unittest.mock import Mock

from rushti.contention_analyzer import (
    ParameterAnalysis,
    ContentionGroup,
    ContentionAnalysisResult,
    _compute_ewma_durations,
    _identify_varying_parameters,
    _find_contention_driver,
    _detect_heavy_outliers,
    _build_predecessor_chains,
    _recommend_max_workers,
    _pearson_correlation,
    _round_to_5,
    _detect_concurrency_ceiling,
    analyze_contention,
    get_archived_taskfile_path,
    write_optimized_taskfile,
    _empty_result,
)


class TestParameterAnalysis(TestCase):
    """Tests for ParameterAnalysis dataclass."""

    def test_create(self):
        pa = ParameterAnalysis(
            key="pSegment",
            distinct_values=3,
            group_averages={"A": 10.0, "B": 20.0, "C": 50.0},
            range_seconds=40.0,
        )
        self.assertEqual(pa.key, "pSegment")
        self.assertEqual(pa.distinct_values, 3)
        self.assertEqual(pa.range_seconds, 40.0)
        self.assertEqual(len(pa.group_averages), 3)


class TestContentionGroup(TestCase):
    """Tests for ContentionGroup dataclass."""

    def test_create(self):
        g = ContentionGroup(
            driver_value="2170",
            task_ids=["1", "2", "3"],
            avg_duration=150.0,
            is_heavy=True,
        )
        self.assertEqual(g.driver_value, "2170")
        self.assertEqual(len(g.task_ids), 3)
        self.assertTrue(g.is_heavy)

    def test_default_is_heavy_false(self):
        g = ContentionGroup(
            driver_value="1000",
            task_ids=["1"],
            avg_duration=5.0,
        )
        self.assertFalse(g.is_heavy)


class TestContentionAnalysisResult(TestCase):
    """Tests for ContentionAnalysisResult dataclass."""

    def test_total_tasks(self):
        heavy = [
            ContentionGroup("A", ["1", "2"], 100.0, True),
            ContentionGroup("B", ["3", "4"], 80.0, True),
        ]
        light = [
            ContentionGroup("C", ["5", "6", "7"], 10.0, False),
        ]
        result = ContentionAnalysisResult(
            contention_driver="pSegment",
            fan_out_keys=["pPeriod"],
            heavy_groups=heavy,
            light_groups=light,
            all_groups=heavy + light,
            chain_length=2,
            fan_out_size=2,
            critical_path_seconds=180.0,
            recommended_workers=4,
            sensitivity=10.0,
            iqr_stats={},
            predecessor_map={},
        )
        self.assertEqual(result.total_tasks, 7)
        self.assertEqual(result.heavy_task_count, 4)
        self.assertEqual(result.light_task_count, 3)

    def test_empty_result_defaults(self):
        result = _empty_result("test warning", 10.0)
        self.assertIsNone(result.contention_driver)
        self.assertEqual(result.total_tasks, 0)
        self.assertEqual(result.warnings, ["test warning"])
        self.assertEqual(result.sensitivity, 10.0)


class TestIdentifyVaryingParameters(TestCase):
    """Tests for _identify_varying_parameters."""

    def test_two_varying_params(self):
        """Two parameters vary across tasks."""
        tasks = [
            {"parameters": {"pSegment": "A", "pPeriod": "1", "pConst": "X"}},
            {"parameters": {"pSegment": "A", "pPeriod": "2", "pConst": "X"}},
            {"parameters": {"pSegment": "B", "pPeriod": "1", "pConst": "X"}},
            {"parameters": {"pSegment": "B", "pPeriod": "2", "pConst": "X"}},
        ]
        varying = _identify_varying_parameters(tasks)
        self.assertEqual(sorted(varying), ["pPeriod", "pSegment"])

    def test_no_varying_params(self):
        """All parameters are constant."""
        tasks = [
            {"parameters": {"pConst": "X"}},
            {"parameters": {"pConst": "X"}},
        ]
        varying = _identify_varying_parameters(tasks)
        self.assertEqual(varying, [])

    def test_single_varying_param(self):
        """One parameter varies, one is constant."""
        tasks = [
            {"parameters": {"pSegment": "A", "pConst": "X"}},
            {"parameters": {"pSegment": "B", "pConst": "X"}},
        ]
        varying = _identify_varying_parameters(tasks)
        self.assertEqual(varying, ["pSegment"])

    def test_empty_task_list(self):
        self.assertEqual(_identify_varying_parameters([]), [])

    def test_empty_parameters(self):
        tasks = [{"parameters": {}}, {"parameters": {}}]
        self.assertEqual(_identify_varying_parameters(tasks), [])

    def test_single_task(self):
        """Single task has no variation."""
        tasks = [{"parameters": {"pSegment": "A", "pPeriod": "1"}}]
        varying = _identify_varying_parameters(tasks)
        self.assertEqual(varying, [])


class TestFindContentionDriver(TestCase):
    """Tests for _find_contention_driver."""

    def _make_tasks_and_ewma(self):
        """Create a task set where pSegment drives duration (like KDP scenario).

        Segments: A (heavy=100s), B (heavy=80s), C (light=10s), D (light=12s)
        Periods: 1, 2, 3 (negligible variance ~2s range)
        """
        tasks = []
        ewma_map = {}
        task_id = 1

        # Duration pattern: segment drives variance, period has tiny effect
        segment_base = {"A": 100.0, "B": 80.0, "C": 10.0, "D": 12.0}
        period_offset = {"1": 0.0, "2": 1.0, "3": 2.0}

        for seg in ["A", "B", "C", "D"]:
            for per in ["1", "2", "3"]:
                sig = f"sig_{seg}_{per}"
                duration = segment_base[seg] + period_offset[per]
                tasks.append(
                    {
                        "task_id": str(task_id),
                        "task_signature": sig,
                        "process": "TestProcess",
                        "parameters": {"pSegment": seg, "pPeriod": per},
                    }
                )
                ewma_map[sig] = duration
                task_id += 1

        return tasks, ewma_map

    def test_clear_contention_driver(self):
        """pSegment should be identified as contention driver (range >> pPeriod range)."""
        tasks, ewma_map = self._make_tasks_and_ewma()
        varying_keys = ["pSegment", "pPeriod"]

        winner, all_analyses = _find_contention_driver(tasks, ewma_map, varying_keys)

        self.assertIsNotNone(winner)
        self.assertEqual(winner.key, "pSegment")
        # Segment range: avg(A)=101 - avg(D)=13 = 88. Period range: avg(3)=52 - avg(1)=50.5 = 1.5
        self.assertGreater(winner.range_seconds, 50)
        self.assertEqual(len(all_analyses), 2)

    def test_ambiguous_signal_returns_none(self):
        """When both parameters have similar ranges, returns None."""
        tasks = [
            {
                "task_id": "1",
                "task_signature": "s1",
                "process": "P",
                "parameters": {"X": "a", "Y": "1"},
            },
            {
                "task_id": "2",
                "task_signature": "s2",
                "process": "P",
                "parameters": {"X": "a", "Y": "2"},
            },
            {
                "task_id": "3",
                "task_signature": "s3",
                "process": "P",
                "parameters": {"X": "b", "Y": "1"},
            },
            {
                "task_id": "4",
                "task_signature": "s4",
                "process": "P",
                "parameters": {"X": "b", "Y": "2"},
            },
        ]
        # Durations: both X and Y have similar range
        ewma_map = {"s1": 10.0, "s2": 20.0, "s3": 15.0, "s4": 25.0}

        winner, _ = _find_contention_driver(tasks, ewma_map, ["X", "Y"], min_range_ratio=5.0)
        self.assertIsNone(winner)

    def test_no_varying_keys_returns_none(self):
        winner, analyses = _find_contention_driver([], {}, [])
        self.assertIsNone(winner)
        self.assertEqual(analyses, [])

    def test_single_varying_key_always_wins(self):
        """With only one varying key, it always wins (no runner-up to compare)."""
        tasks = [
            {"task_id": "1", "task_signature": "s1", "process": "P", "parameters": {"X": "a"}},
            {"task_id": "2", "task_signature": "s2", "process": "P", "parameters": {"X": "b"}},
        ]
        ewma_map = {"s1": 10.0, "s2": 100.0}

        winner, _ = _find_contention_driver(tasks, ewma_map, ["X"])
        self.assertIsNotNone(winner)
        self.assertEqual(winner.key, "X")
        self.assertAlmostEqual(winner.range_seconds, 90.0)

    def test_tasks_missing_from_ewma_map(self):
        """Tasks without EWMA data should be gracefully skipped."""
        tasks = [
            {"task_id": "1", "task_signature": "s1", "process": "P", "parameters": {"X": "a"}},
            {"task_id": "2", "task_signature": "s2", "process": "P", "parameters": {"X": "b"}},
        ]
        # Only s1 has EWMA data
        ewma_map = {"s1": 10.0}

        winner, _ = _find_contention_driver(tasks, ewma_map, ["X"])
        # Should still work with the available data
        self.assertIsNotNone(winner)


class TestDetectHeavyOutliers(TestCase):
    """Tests for _detect_heavy_outliers."""

    def _make_groups(self, durations):
        """Create groups from a list of (name, duration) tuples."""
        return [
            ContentionGroup(
                driver_value=name,
                task_ids=[f"t_{name}_{i}" for i in range(3)],
                avg_duration=dur,
            )
            for name, dur in durations
        ]

    def test_clear_outliers_with_default_sensitivity(self):
        """With k=10, only extreme outliers are detected."""
        # 60 "normal" groups around 10-20s, 3 "heavy" groups at 150-200s
        groups = self._make_groups(
            [(f"light_{i}", 10.0 + i * 0.2) for i in range(60)]
            + [("heavy_1", 200.0), ("heavy_2", 180.0), ("heavy_3", 150.0)]
        )
        heavy, light, stats = _detect_heavy_outliers(groups, sensitivity=10.0)

        # Should detect 3 outliers
        self.assertEqual(len(heavy), 3)
        self.assertEqual(len(light), 60)
        self.assertEqual(heavy[0].driver_value, "heavy_1")
        self.assertTrue(heavy[0].is_heavy)
        self.assertFalse(light[0].is_heavy)

    def test_textbook_iqr_catches_more(self):
        """With k=1.5 (textbook IQR), more groups are flagged as heavy."""
        groups = self._make_groups(
            [(f"light_{i}", 10.0 + i * 0.2) for i in range(50)]
            + [(f"mid_{i}", 40.0 + i * 5) for i in range(8)]
            + [("heavy_1", 200.0), ("heavy_2", 180.0)]
        )
        heavy, light, stats = _detect_heavy_outliers(groups, sensitivity=1.5)

        # Should catch more than just the 2 heaviest
        self.assertGreater(len(heavy), 2)

    def test_too_few_groups(self):
        """With fewer than 4 groups, no outlier detection is performed."""
        groups = self._make_groups([("A", 100.0), ("B", 10.0), ("C", 20.0)])
        heavy, light, stats = _detect_heavy_outliers(groups, sensitivity=10.0)

        self.assertEqual(len(heavy), 0)
        self.assertEqual(len(light), 3)

    def test_all_identical_durations(self):
        """When all groups have the same duration, IQR=0, no outliers."""
        groups = self._make_groups([(f"g_{i}", 10.0) for i in range(10)])
        heavy, light, stats = _detect_heavy_outliers(groups, sensitivity=10.0)

        self.assertEqual(len(heavy), 0)
        self.assertEqual(len(light), 10)
        self.assertEqual(stats["iqr"], 0.0)

    def test_heavy_sorted_descending(self):
        """Heavy groups are sorted by duration descending."""
        groups = self._make_groups(
            [(f"light_{i}", 10.0) for i in range(20)] + [("mid", 150.0), ("heavy", 200.0)]
        )
        heavy, _, _ = _detect_heavy_outliers(groups, sensitivity=1.5)

        if len(heavy) >= 2:
            self.assertGreaterEqual(heavy[0].avg_duration, heavy[1].avg_duration)

    def test_iqr_stats_populated(self):
        """IQR stats dict contains q1, q3, iqr, upper_fence."""
        groups = self._make_groups([(f"g_{i}", float(10 + i)) for i in range(20)])
        _, _, stats = _detect_heavy_outliers(groups, sensitivity=10.0)

        self.assertIn("q1", stats)
        self.assertIn("q3", stats)
        self.assertIn("iqr", stats)
        self.assertIn("upper_fence", stats)
        self.assertGreaterEqual(stats["q3"], stats["q1"])
        self.assertGreater(stats["upper_fence"], stats["q3"])


class TestBuildPredecessorChains(TestCase):
    """Tests for _build_predecessor_chains."""

    def test_basic_chain_two_heavy_groups(self):
        """Two heavy groups produce chains across all fan-out values."""
        heavy_groups = [
            ContentionGroup("heavy_1", [], 200.0, True),
            ContentionGroup("heavy_2", [], 150.0, True),
        ]
        # 2 segments × 3 periods = 6 tasks
        task_params = [
            {"task_id": "1", "parameters": {"pSeg": "heavy_1", "pPer": "1"}},
            {"task_id": "2", "parameters": {"pSeg": "heavy_1", "pPer": "2"}},
            {"task_id": "3", "parameters": {"pSeg": "heavy_1", "pPer": "3"}},
            {"task_id": "4", "parameters": {"pSeg": "heavy_2", "pPer": "1"}},
            {"task_id": "5", "parameters": {"pSeg": "heavy_2", "pPer": "2"}},
            {"task_id": "6", "parameters": {"pSeg": "heavy_2", "pPer": "3"}},
        ]

        predecessor_map = _build_predecessor_chains(heavy_groups, task_params, "pSeg", ["pPer"])

        # Each task in heavy_2 should have a predecessor from heavy_1
        # (same pPer value)
        self.assertEqual(len(predecessor_map), 3)
        self.assertEqual(predecessor_map["4"], ["1"])  # pPer=1
        self.assertEqual(predecessor_map["5"], ["2"])  # pPer=2
        self.assertEqual(predecessor_map["6"], ["3"])  # pPer=3

    def test_three_heavy_groups_chain(self):
        """Three heavy groups produce chains of length 2 per fan-out value."""
        heavy_groups = [
            ContentionGroup("H1", [], 200.0, True),
            ContentionGroup("H2", [], 150.0, True),
            ContentionGroup("H3", [], 120.0, True),
        ]
        task_params = [
            {"task_id": "1", "parameters": {"pSeg": "H1", "pPer": "1"}},
            {"task_id": "2", "parameters": {"pSeg": "H2", "pPer": "1"}},
            {"task_id": "3", "parameters": {"pSeg": "H3", "pPer": "1"}},
            {"task_id": "4", "parameters": {"pSeg": "H1", "pPer": "2"}},
            {"task_id": "5", "parameters": {"pSeg": "H2", "pPer": "2"}},
            {"task_id": "6", "parameters": {"pSeg": "H3", "pPer": "2"}},
        ]

        predecessor_map = _build_predecessor_chains(heavy_groups, task_params, "pSeg", ["pPer"])

        # For pPer=1: H2 depends on H1, H3 depends on H2
        self.assertEqual(predecessor_map["2"], ["1"])  # H2→H1 for pPer=1
        self.assertEqual(predecessor_map["3"], ["2"])  # H3→H2 for pPer=1
        # For pPer=2: H2 depends on H1, H3 depends on H2
        self.assertEqual(predecessor_map["5"], ["4"])  # H2→H1 for pPer=2
        self.assertEqual(predecessor_map["6"], ["5"])  # H3→H2 for pPer=2
        self.assertEqual(len(predecessor_map), 4)

    def test_single_heavy_group_no_chain(self):
        """Single heavy group produces no predecessors."""
        heavy_groups = [ContentionGroup("H1", [], 200.0, True)]
        task_params = [
            {"task_id": "1", "parameters": {"pSeg": "H1", "pPer": "1"}},
        ]

        predecessor_map = _build_predecessor_chains(heavy_groups, task_params, "pSeg", ["pPer"])
        self.assertEqual(predecessor_map, {})

    def test_empty_heavy_groups(self):
        predecessor_map = _build_predecessor_chains([], [], "pSeg", ["pPer"])
        self.assertEqual(predecessor_map, {})


class TestRecommendMaxWorkers(TestCase):
    """Tests for _recommend_max_workers."""

    def test_basic_recommendation(self):
        """Formula: chain_slots + ceil(light_work / chain_duration)."""
        heavy_groups = [
            ContentionGroup("H1", ["t1", "t2"], 200.0, True),
            ContentionGroup("H2", ["t3", "t4"], 100.0, True),
        ]
        light_groups = [
            ContentionGroup("L1", ["t5", "t6", "t7", "t8"], 10.0, False),
            ContentionGroup("L2", ["t9", "t10", "t11", "t12"], 10.0, False),
        ]
        fan_out_size = 2

        recommended = _recommend_max_workers(heavy_groups, light_groups, fan_out_size)

        # chain_slots = 2
        # critical_path = 200 + 100 = 300s
        # light_total_work = 8 tasks × 10s = 80s
        # light_slots = ceil(80 / 300) = 1
        # total = 2 + 1 = 3
        self.assertEqual(recommended, 3)

    def test_no_heavy_groups(self):
        """No heavy groups — workers based on light work volume."""
        light_groups = [
            ContentionGroup("L1", ["t1", "t2"], 10.0, False),
        ]
        recommended = _recommend_max_workers([], light_groups, fan_out_size=5)
        # No chains, so critical_path=1.0 (fallback)
        # light_total_work = 2 * 10 = 20, light_slots = ceil(20/1) = 20
        # total = 5 + 20 = 25 → but floor = fan_out_size = 5
        self.assertGreaterEqual(recommended, 5)

    def test_no_light_groups(self):
        """No light groups means recommended = fan_out_size."""
        heavy_groups = [
            ContentionGroup("H1", ["t1"], 200.0, True),
            ContentionGroup("H2", ["t2"], 100.0, True),
        ]
        recommended = _recommend_max_workers(heavy_groups, [], fan_out_size=12)
        self.assertEqual(recommended, 12)

    def test_many_light_tasks(self):
        """Many light tasks increase worker recommendation."""
        heavy_groups = [
            ContentionGroup("H1", ["t1"] * 12, 100.0, True),
            ContentionGroup("H2", ["t2"] * 12, 50.0, True),
        ]
        light_groups = [ContentionGroup(f"L{i}", [f"t{i}"] * 12, 10.0, False) for i in range(50)]
        fan_out_size = 12

        recommended = _recommend_max_workers(heavy_groups, light_groups, fan_out_size)

        # critical_path = 150s
        # light_total_work = 50 * 12 * 10 = 6000s
        # light_slots = ceil(6000 / 150) = 40
        # total = 12 + 40 = 52
        self.assertGreater(recommended, fan_out_size)

    def test_floor_at_fan_out_size(self):
        """Result is never less than fan_out_size."""
        heavy_groups = [
            ContentionGroup("H1", ["t1"], 100.0, True),
            ContentionGroup("H2", ["t2"], 100.0, True),
        ]
        recommended = _recommend_max_workers(heavy_groups, [], fan_out_size=20)
        self.assertGreaterEqual(recommended, 20)


class TestComputeEwmaDurations(TestCase):
    """Tests for _compute_ewma_durations."""

    def test_basic_ewma(self):
        """EWMA is computed correctly for simple case."""
        stats_db = Mock()
        stats_db.get_workflow_signatures.return_value = ["sig1"]
        stats_db.get_task_durations.return_value = [10.0, 10.0, 10.0]

        result = _compute_ewma_durations(stats_db, "test_workflow")

        self.assertIn("sig1", result)
        # With constant durations, EWMA should be close to 10
        self.assertAlmostEqual(result["sig1"], 10.0, places=1)

    def test_ewma_weights_recent(self):
        """EWMA gives more weight to recent values."""
        stats_db = Mock()
        stats_db.get_workflow_signatures.return_value = ["sig1"]
        # Most recent = 100, older = 10
        stats_db.get_task_durations.return_value = [100.0, 10.0, 10.0]

        result = _compute_ewma_durations(stats_db, "test_workflow", alpha=0.3)

        # EWMA should be closer to 100 (recent) than 10 (old)
        self.assertGreater(result["sig1"], 50)

    def test_outlier_dampening(self):
        """Outliers > 3x EWMA are capped at 2x."""
        stats_db = Mock()
        stats_db.get_workflow_signatures.return_value = ["sig1"]
        # durations[0]=10, durations[1]=100 (100 > 10*3, capped to 10*2=20)
        stats_db.get_task_durations.return_value = [10.0, 100.0]

        result = _compute_ewma_durations(stats_db, "test_workflow", alpha=0.3)

        # Without dampening: ewma = 0.3*100 + 0.7*10 = 37
        # With dampening: ewma = 0.3*20 + 0.7*10 = 13
        self.assertLess(result["sig1"], 20)

    def test_empty_workflow(self):
        stats_db = Mock()
        stats_db.get_workflow_signatures.return_value = []

        result = _compute_ewma_durations(stats_db, "empty_workflow")
        self.assertEqual(result, {})

    def test_no_durations(self):
        stats_db = Mock()
        stats_db.get_workflow_signatures.return_value = ["sig1"]
        stats_db.get_task_durations.return_value = []

        result = _compute_ewma_durations(stats_db, "test_workflow")
        self.assertEqual(result, {})

    def test_multiple_signatures(self):
        stats_db = Mock()
        stats_db.get_workflow_signatures.return_value = ["sig1", "sig2"]
        stats_db.get_task_durations.side_effect = [[10.0], [20.0]]

        result = _compute_ewma_durations(stats_db, "test_workflow")

        self.assertEqual(len(result), 2)
        self.assertAlmostEqual(result["sig1"], 10.0)
        self.assertAlmostEqual(result["sig2"], 20.0)


class TestAnalyzeContention(TestCase):
    """Integration tests for analyze_contention (main entry point)."""

    def _create_mock_stats_db(self, task_rows, ewma_durations):
        """Create a mock StatsDatabase with the given data.

        :param task_rows: List of dicts with task_id, task_signature, process, parameters
        :param ewma_durations: Dict of {signature: [durations]} for EWMA
        """
        stats_db = Mock()
        stats_db.enabled = True

        # Mock _conn.cursor() for _get_task_parameters
        mock_cursor = Mock()
        mock_rows = []
        for row in task_rows:
            mock_row = {
                "task_id": row["task_id"],
                "task_signature": row["task_signature"],
                "process": row["process"],
                "parameters": json.dumps(row["parameters"]),
            }
            mock_rows.append(mock_row)
        mock_cursor.fetchall.return_value = mock_rows
        stats_db._conn.cursor.return_value = mock_cursor

        # Mock get_workflow_signatures
        stats_db.get_workflow_signatures.return_value = list(ewma_durations.keys())

        # Mock get_task_durations
        def get_durations(sig, limit=10):
            return ewma_durations.get(sig, [])

        stats_db.get_task_durations.side_effect = get_durations

        # Mock ceiling-detection methods (default: no runs for ceiling analysis)
        stats_db.get_runs_for_workflow.return_value = []

        return stats_db

    def test_kdp_like_workflow(self):
        """Simulates a KDP-like workflow: 10 segments × 3 periods.

        Segments S8, S9 are heavy (~200s, ~180s), S0-S7 are light (~10-17s).
        pPeriod has negligible variance (~1s).
        """
        task_rows = []
        ewma_durations = {}
        task_id = 1

        # 8 light segments (10-17s) + 2 heavy segments (200, 180s)
        segment_base = {f"S{i}": 10.0 + i for i in range(8)}
        segment_base["S8"] = 200.0
        segment_base["S9"] = 180.0

        for seg in sorted(segment_base.keys()):
            for per in ["1", "2", "3"]:
                sig = f"sig_{seg}_{per}"
                dur = segment_base[seg] + float(per)
                task_rows.append(
                    {
                        "task_id": str(task_id),
                        "task_signature": sig,
                        "process": "ProcessKDP",
                        "parameters": {"pSegment": seg, "pPeriod": per},
                    }
                )
                ewma_durations[sig] = [dur]
                task_id += 1

        stats_db = self._create_mock_stats_db(task_rows, ewma_durations)
        result = analyze_contention(stats_db, "test_workflow", sensitivity=1.5)

        self.assertEqual(result.contention_driver, "pSegment")
        self.assertIn("pPeriod", result.fan_out_keys)
        self.assertEqual(result.fan_out_size, 3)
        # S8 and S9 are heavy outliers
        self.assertGreaterEqual(len(result.heavy_groups), 2)
        heavy_values = {g.driver_value for g in result.heavy_groups}
        self.assertIn("S8", heavy_values)
        self.assertIn("S9", heavy_values)

    def test_no_history(self):
        """No historical data returns empty result with warning."""
        stats_db = Mock()
        stats_db.enabled = True
        stats_db.get_workflow_signatures.return_value = []
        stats_db.get_task_durations.return_value = []

        result = analyze_contention(stats_db, "empty_workflow")
        self.assertIsNone(result.contention_driver)
        self.assertGreater(len(result.warnings), 0)

    def test_no_varying_parameters(self):
        """All tasks have identical parameters."""
        task_rows = [
            {"task_id": "1", "task_signature": "s1", "process": "P", "parameters": {"X": "a"}},
            {"task_id": "2", "task_signature": "s2", "process": "P", "parameters": {"X": "a"}},
        ]
        ewma_durations = {"s1": [10.0], "s2": [20.0]}
        stats_db = self._create_mock_stats_db(task_rows, ewma_durations)

        result = analyze_contention(stats_db, "test_workflow")
        self.assertIsNone(result.contention_driver)
        self.assertIn("identical parameters", result.warnings[0])

    def test_sensitivity_affects_outlier_count(self):
        """Higher sensitivity → fewer outliers detected."""
        task_rows = []
        ewma_durations = {}
        task_id = 1

        # 20 groups: 17 light (10-27s), 3 heavy (100, 120, 140s)
        for i in range(20):
            if i < 17:
                dur = 10.0 + i
            else:
                dur = 100.0 + (i - 17) * 20
            for per in ["1", "2"]:
                sig = f"sig_{i}_{per}"
                task_rows.append(
                    {
                        "task_id": str(task_id),
                        "task_signature": sig,
                        "process": "P",
                        "parameters": {"pSeg": str(i), "pPer": per},
                    }
                )
                ewma_durations[sig] = [dur]
                task_id += 1

        stats_db_low = self._create_mock_stats_db(task_rows, ewma_durations)
        stats_db_high = self._create_mock_stats_db(task_rows, ewma_durations)

        result_low = analyze_contention(stats_db_low, "wf", sensitivity=1.5)
        result_high = analyze_contention(stats_db_high, "wf", sensitivity=10.0)

        # Lower sensitivity catches more outliers
        self.assertGreaterEqual(len(result_low.heavy_groups), len(result_high.heavy_groups))

    def test_predecessor_chains_generated(self):
        """When heavy groups are detected, predecessor chains are generated."""
        task_rows = []
        ewma_durations = {}
        task_id = 1

        # 10 segments: 8 light (~10s), 2 heavy (~200s, ~180s)
        for i in range(10):
            dur = 10.0 + i if i < 8 else (200.0 if i == 8 else 180.0)
            for per in ["1", "2", "3"]:
                sig = f"sig_{i}_{per}"
                task_rows.append(
                    {
                        "task_id": str(task_id),
                        "task_signature": sig,
                        "process": "P",
                        "parameters": {"pSeg": str(i), "pPer": per},
                    }
                )
                ewma_durations[sig] = [dur]
                task_id += 1

        stats_db = self._create_mock_stats_db(task_rows, ewma_durations)
        result = analyze_contention(stats_db, "wf", sensitivity=1.5)

        if len(result.heavy_groups) >= 2:
            # Should have predecessor chains (fan_out_size chains)
            self.assertGreater(len(result.predecessor_map), 0)
            # Each chain across fan_out produces 1 predecessor per fan-out value
            self.assertEqual(
                len(result.predecessor_map),
                result.fan_out_size * (result.chain_length - 1),
            )

    def test_recommended_workers_positive(self):
        """Recommended workers is always positive when there are tasks."""
        task_rows = []
        ewma_durations = {}
        task_id = 1
        for i in range(5):
            dur = 10.0 + i * 50
            for per in ["1", "2"]:
                sig = f"sig_{i}_{per}"
                task_rows.append(
                    {
                        "task_id": str(task_id),
                        "task_signature": sig,
                        "process": "P",
                        "parameters": {"pSeg": str(i), "pPer": per},
                    }
                )
                ewma_durations[sig] = [dur]
                task_id += 1

        stats_db = self._create_mock_stats_db(task_rows, ewma_durations)
        result = analyze_contention(stats_db, "wf", sensitivity=1.5)

        if result.contention_driver:
            self.assertGreater(result.recommended_workers, 0)


class TestWriteOptimizedTaskfile(TestCase):
    """Tests for write_optimized_taskfile."""

    def _create_taskfile(self, tmp_dir: str) -> str:
        """Create a minimal JSON taskfile for testing."""
        taskfile_data = {
            "version": "2.0",
            "metadata": {
                "workflow": "test_workflow",
                "name": "",
                "description": "test taskfile",
                "author": "",
            },
            "settings": {},
            "tasks": [
                {
                    "id": "1",
                    "instance": "tm1srv01",
                    "process": "TestProcess",
                    "parameters": {"pSegment": "A", "pPeriod": "1"},
                },
                {
                    "id": "2",
                    "instance": "tm1srv01",
                    "process": "TestProcess",
                    "parameters": {"pSegment": "A", "pPeriod": "2"},
                },
                {
                    "id": "3",
                    "instance": "tm1srv01",
                    "process": "TestProcess",
                    "parameters": {"pSegment": "B", "pPeriod": "1"},
                },
                {
                    "id": "4",
                    "instance": "tm1srv01",
                    "process": "TestProcess",
                    "parameters": {"pSegment": "B", "pPeriod": "2"},
                },
            ],
        }
        path = os.path.join(tmp_dir, "test_taskfile.json")
        with open(path, "w") as f:
            json.dump(taskfile_data, f)
        return path

    def test_write_with_predecessors(self):
        """Optimized taskfile has predecessors injected."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            taskfile_path = self._create_taskfile(tmp_dir)
            output_path = os.path.join(tmp_dir, "optimized.json")

            result = ContentionAnalysisResult(
                contention_driver="pSegment",
                fan_out_keys=["pPeriod"],
                heavy_groups=[
                    ContentionGroup("A", ["1", "2"], 100.0, True),
                    ContentionGroup("B", ["3", "4"], 80.0, True),
                ],
                light_groups=[],
                all_groups=[
                    ContentionGroup("A", ["1", "2"], 100.0, True),
                    ContentionGroup("B", ["3", "4"], 80.0, True),
                ],
                chain_length=2,
                fan_out_size=2,
                critical_path_seconds=180.0,
                recommended_workers=4,
                sensitivity=10.0,
                iqr_stats={"q1": 10.0, "q3": 20.0, "iqr": 10.0, "upper_fence": 120.0},
                predecessor_map={"3": ["1"], "4": ["2"]},
            )

            write_optimized_taskfile(taskfile_path, result, output_path)

            # Verify output file exists
            self.assertTrue(os.path.exists(output_path))

            # Read and verify
            with open(output_path) as f:
                optimized = json.load(f)

            # Check predecessors were applied
            tasks_by_id = {t["id"]: t for t in optimized["tasks"]}
            self.assertEqual(tasks_by_id["3"].get("predecessors", []), ["1"])
            self.assertEqual(tasks_by_id["4"].get("predecessors", []), ["2"])

            # Check max_workers embedded in settings
            self.assertEqual(optimized["settings"]["max_workers"], 4)

    def test_write_reorders_tasks(self):
        """Tasks are reordered to contention-driver-major."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            taskfile_path = self._create_taskfile(tmp_dir)
            output_path = os.path.join(tmp_dir, "optimized.json")

            result = ContentionAnalysisResult(
                contention_driver="pSegment",
                fan_out_keys=["pPeriod"],
                heavy_groups=[
                    ContentionGroup("B", ["3", "4"], 100.0, True),
                ],
                light_groups=[
                    ContentionGroup("A", ["1", "2"], 10.0, False),
                ],
                all_groups=[
                    ContentionGroup("B", ["3", "4"], 100.0, True),
                    ContentionGroup("A", ["1", "2"], 10.0, False),
                ],
                chain_length=1,
                fan_out_size=2,
                critical_path_seconds=100.0,
                recommended_workers=2,
                sensitivity=10.0,
                iqr_stats={},
                predecessor_map={},
            )

            write_optimized_taskfile(taskfile_path, result, output_path)

            with open(output_path) as f:
                optimized = json.load(f)

            # B (heavy) should come before A (light)
            task_ids = [t["id"] for t in optimized["tasks"]]
            b_indices = [i for i, tid in enumerate(task_ids) if tid in ("3", "4")]
            a_indices = [i for i, tid in enumerate(task_ids) if tid in ("1", "2")]
            self.assertTrue(max(b_indices) < min(a_indices))

    def test_write_updates_metadata(self):
        """Metadata description is updated with optimization info."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            taskfile_path = self._create_taskfile(tmp_dir)
            output_path = os.path.join(tmp_dir, "optimized.json")

            result = ContentionAnalysisResult(
                contention_driver="pSegment",
                fan_out_keys=["pPeriod"],
                heavy_groups=[],
                light_groups=[
                    ContentionGroup("A", ["1", "2"], 10.0, False),
                    ContentionGroup("B", ["3", "4"], 10.0, False),
                ],
                all_groups=[],
                chain_length=0,
                fan_out_size=2,
                critical_path_seconds=0.0,
                recommended_workers=2,
                sensitivity=10.0,
                iqr_stats={},
                predecessor_map={},
            )

            write_optimized_taskfile(taskfile_path, result, output_path)

            with open(output_path) as f:
                optimized = json.load(f)

            desc = optimized["metadata"]["description"]
            self.assertIn("Contention-aware", desc)
            self.assertIn("pSegment", desc)

            # Check max_workers embedded in settings
            self.assertEqual(optimized["settings"]["max_workers"], 2)


class TestGetArchivedTaskfilePath(TestCase):
    """Tests for get_archived_taskfile_path."""

    def test_returns_path_for_successful_run(self):
        """Returns taskfile_path from the most recent successful run."""
        stats_db = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {"taskfile_path": "/archive/my_workflow/run123.json"}
        stats_db._conn.cursor.return_value = mock_cursor

        result = get_archived_taskfile_path(stats_db, "my_workflow")
        self.assertEqual(result, "/archive/my_workflow/run123.json")

    def test_returns_none_when_no_runs(self):
        """Returns None when no successful runs exist."""
        stats_db = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        stats_db._conn.cursor.return_value = mock_cursor

        result = get_archived_taskfile_path(stats_db, "empty_workflow")
        self.assertIsNone(result)

    def test_returns_none_when_path_is_null(self):
        """Returns None when taskfile_path is NULL in the database."""
        stats_db = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {"taskfile_path": None}
        stats_db._conn.cursor.return_value = mock_cursor

        result = get_archived_taskfile_path(stats_db, "workflow_with_null")
        self.assertIsNone(result)


class TestPearsonCorrelation(TestCase):
    """Tests for _pearson_correlation helper."""

    def test_perfect_positive(self):
        """Perfect positive correlation returns ~1.0."""
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        ys = [10.0, 20.0, 30.0, 40.0, 50.0]
        r = _pearson_correlation(xs, ys)
        self.assertAlmostEqual(r, 1.0, places=5)

    def test_perfect_negative(self):
        """Perfect negative correlation returns ~-1.0."""
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        ys = [50.0, 40.0, 30.0, 20.0, 10.0]
        r = _pearson_correlation(xs, ys)
        self.assertAlmostEqual(r, -1.0, places=5)

    def test_no_correlation(self):
        """No correlation returns near 0."""
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        ys = [5.0, 1.0, 5.0, 1.0, 5.0]
        r = _pearson_correlation(xs, ys)
        self.assertAlmostEqual(r, 0.0, places=1)

    def test_too_few_points(self):
        """Fewer than 3 points returns 0.0."""
        self.assertEqual(_pearson_correlation([1.0, 2.0], [3.0, 4.0]), 0.0)
        self.assertEqual(_pearson_correlation([], []), 0.0)

    def test_constant_values(self):
        """Constant values (zero variance) returns 0.0."""
        xs = [5.0, 5.0, 5.0, 5.0]
        ys = [1.0, 2.0, 3.0, 4.0]
        r = _pearson_correlation(xs, ys)
        self.assertEqual(r, 0.0)


class TestRoundTo5(TestCase):
    """Tests for _round_to_5 helper."""

    def test_round_down(self):
        self.assertEqual(_round_to_5(17.4), 15)

    def test_round_up(self):
        self.assertEqual(_round_to_5(17.7), 20)

    def test_exact(self):
        self.assertEqual(_round_to_5(30.0), 30)

    def test_minimum_is_5(self):
        self.assertEqual(_round_to_5(1.0), 5)
        self.assertEqual(_round_to_5(0.0), 5)

    def test_large_value(self):
        self.assertEqual(_round_to_5(29.9), 30)


class TestDetectConcurrencyCeiling(TestCase):
    """Tests for _detect_concurrency_ceiling."""

    def _make_mock_db(
        self,
        runs=None,
        task_stats=None,
        concurrent_data=None,
    ):
        """Create a mock StatsDatabase for ceiling detection.

        :param runs: List of run dicts (for get_runs_for_workflow)
        :param task_stats: Dict or None (for get_run_task_stats)
        :param concurrent_data: List of dicts (for get_concurrent_task_counts)
        """
        db = Mock()
        db.enabled = True
        db.get_runs_for_workflow.return_value = runs or []

        if task_stats is not None:
            db.get_run_task_stats.return_value = task_stats
        else:
            db.get_run_task_stats.return_value = None

        db.get_concurrent_task_counts.return_value = concurrent_data or []
        return db

    def test_no_runs_returns_none(self):
        """No runs → no ceiling."""
        db = self._make_mock_db(runs=[])
        result = _detect_concurrency_ceiling(db, "wf")
        self.assertIsNone(result)

    def test_single_run_no_data_returns_none(self):
        """Single run but no task stats → no ceiling."""
        db = self._make_mock_db(
            runs=[
                {
                    "run_id": "r1",
                    "status": "Success",
                    "max_workers": 50,
                    "duration_seconds": 100.0,
                }
            ],
            task_stats=None,
            concurrent_data=[],
        )
        result = _detect_concurrency_ceiling(db, "wf")
        self.assertIsNone(result)

    def test_single_run_high_correlation_low_efficiency(self):
        """Strong correlation + low efficiency → ceiling detected."""
        # Simulate: tasks at higher concurrency take proportionally longer
        concurrent_data = []
        for i in range(50):
            # concurrency 40-90, duration 100-500 (strong positive correlation)
            conc = 40 + i
            dur = 100.0 + i * 8.0  # linear relationship
            concurrent_data.append(
                {
                    "task_signature": f"sig_{i}",
                    "duration_seconds": dur,
                    "concurrent_count": conc,
                }
            )

        db = self._make_mock_db(
            runs=[
                {
                    "run_id": "r1",
                    "status": "Success",
                    "max_workers": 50,
                    "duration_seconds": 600.0,
                }
            ],
            task_stats={
                "total_duration": 15000.0,  # effective_parallelism = 25
                "task_count": 50,
                "avg_duration": 300.0,
            },
            concurrent_data=concurrent_data,
        )
        result = _detect_concurrency_ceiling(db, "wf")

        self.assertIsNotNone(result)
        self.assertEqual(result["confidence"], "single_run")
        self.assertEqual(result["ceiling_workers"], 25)
        self.assertGreater(result["correlation"], 0.7)
        self.assertLess(result["efficiency"], 0.75)

    def test_single_run_low_correlation_returns_none(self):
        """Weak correlation → no ceiling detected."""
        # Random-ish relationship between concurrency and duration
        concurrent_data = [
            {
                "task_signature": f"s{i}",
                "duration_seconds": 50 + (i % 3) * 10,
                "concurrent_count": 20 + i,
            }
            for i in range(20)
        ]

        db = self._make_mock_db(
            runs=[
                {
                    "run_id": "r1",
                    "status": "Success",
                    "max_workers": 30,
                    "duration_seconds": 200.0,
                }
            ],
            task_stats={
                "total_duration": 4000.0,  # eff_par = 20
                "task_count": 20,
                "avg_duration": 200.0,
            },
            concurrent_data=concurrent_data,
        )
        result = _detect_concurrency_ceiling(db, "wf")
        self.assertIsNone(result)

    def test_single_run_high_efficiency_returns_none(self):
        """High efficiency means server handles concurrency fine → no ceiling."""
        concurrent_data = [
            {
                "task_signature": f"s{i}",
                "duration_seconds": 100 + i * 10,
                "concurrent_count": 10 + i,
            }
            for i in range(20)
        ]

        db = self._make_mock_db(
            runs=[
                {
                    "run_id": "r1",
                    "status": "Success",
                    "max_workers": 20,
                    "duration_seconds": 200.0,
                }
            ],
            task_stats={
                "total_duration": 3600.0,  # eff_par = 18, efficiency = 18/20 = 0.90
                "task_count": 20,
                "avg_duration": 180.0,
            },
            concurrent_data=concurrent_data,
        )
        result = _detect_concurrency_ceiling(db, "wf")
        self.assertIsNone(result)

    def test_multi_run_fewer_workers_faster(self):
        """Fewer workers with shorter wall clock → ceiling confirmed."""
        runs = [
            {  # Most recent first (30 workers, faster)
                "run_id": "r2",
                "status": "Success",
                "max_workers": 30,
                "duration_seconds": 600.0,
            },
            {  # Older (50 workers, slower)
                "run_id": "r1",
                "status": "Success",
                "max_workers": 50,
                "duration_seconds": 800.0,
            },
        ]

        db = self._make_mock_db(runs=runs)

        # Mock get_run_task_stats per run_id
        def get_task_stats(run_id):
            if run_id == "r2":
                return {
                    "total_duration": 10000.0,
                    "task_count": 70,
                    "avg_duration": 142.9,
                }
            elif run_id == "r1":
                return {
                    "total_duration": 20000.0,
                    "task_count": 70,
                    "avg_duration": 285.7,
                }
            return None

        db.get_run_task_stats.side_effect = get_task_stats

        result = _detect_concurrency_ceiling(db, "wf")

        self.assertIsNotNone(result)
        self.assertEqual(result["confidence"], "multi_run")
        # Best run: 30w, eff_par = 10000/600 = 16.7, rounded to 15
        self.assertEqual(result["ceiling_workers"], _round_to_5(10000.0 / 600.0))
        self.assertGreater(result["wall_clock_improvement"], 0)

    def test_multi_run_normal_scaling_returns_none(self):
        """More workers = faster wall clock → normal scaling, no ceiling."""
        runs = [
            {
                "run_id": "r2",
                "status": "Success",
                "max_workers": 50,
                "duration_seconds": 400.0,
            },
            {
                "run_id": "r1",
                "status": "Success",
                "max_workers": 30,
                "duration_seconds": 600.0,
            },
        ]

        db = self._make_mock_db(runs=runs)

        def get_task_stats(run_id):
            if run_id == "r2":
                return {"total_duration": 8000.0, "task_count": 70, "avg_duration": 114.3}
            elif run_id == "r1":
                return {"total_duration": 9000.0, "task_count": 70, "avg_duration": 128.6}
            return None

        db.get_run_task_stats.side_effect = get_task_stats

        result = _detect_concurrency_ceiling(db, "wf")
        self.assertIsNone(result)

    def test_failed_runs_ignored(self):
        """Only successful runs are considered."""
        runs = [
            {"run_id": "r1", "status": "Failed", "max_workers": 50, "duration_seconds": 100.0},
        ]
        db = self._make_mock_db(runs=runs)
        result = _detect_concurrency_ceiling(db, "wf")
        self.assertIsNone(result)

    def test_multi_run_scale_up_detected(self):
        """More workers was faster AND most recent has fewer workers → scale_up."""
        runs = [
            {  # Most recent first (5 workers, slowest — workers were reduced too aggressively)
                "run_id": "r4",
                "status": "Success",
                "max_workers": 5,
                "duration_seconds": 734.0,
            },
            {
                "run_id": "r3",
                "status": "Success",
                "max_workers": 10,
                "duration_seconds": 581.0,
            },
            {
                "run_id": "r2",
                "status": "Success",
                "max_workers": 24,
                "duration_seconds": 692.0,
            },
            {  # Oldest (50 workers, fastest)
                "run_id": "r1",
                "status": "Success",
                "max_workers": 50,
                "duration_seconds": 547.0,
            },
        ]

        db = self._make_mock_db(runs=runs)

        def get_task_stats(run_id):
            stats_map = {
                "r4": {"total_duration": 2952.0, "task_count": 79, "avg_duration": 37.4},
                "r3": {"total_duration": 3489.0, "task_count": 79, "avg_duration": 44.2},
                "r2": {"total_duration": 5872.0, "task_count": 79, "avg_duration": 74.3},
                "r1": {"total_duration": 5684.0, "task_count": 79, "avg_duration": 71.9},
            }
            return stats_map.get(run_id)

        db.get_run_task_stats.side_effect = get_task_stats

        result = _detect_concurrency_ceiling(db, "wf")

        self.assertIsNotNone(result)
        self.assertEqual(result["confidence"], "scale_up")
        # Sweet spot: 10w (581s) is within 10% of best (547s), uses fewest workers
        self.assertEqual(result["ceiling_workers"], 10)
        self.assertGreater(result["wall_clock_improvement"], 0)
        # best_level = sweet spot (10w), worst_level = current (5w)
        self.assertEqual(result["best_level"]["max_workers"], 10)
        self.assertEqual(result["worst_level"]["max_workers"], 5)
        # Improvement: 734 - 581 = 153s
        self.assertAlmostEqual(result["wall_clock_improvement"], 153.0, places=0)

    def test_multi_run_scale_up_not_triggered_when_most_recent_is_best(self):
        """More workers faster but most recent already at best level → no scale_up."""
        runs = [
            {  # Most recent: already at optimal (50 workers, fastest)
                "run_id": "r2",
                "status": "Success",
                "max_workers": 50,
                "duration_seconds": 400.0,
            },
            {
                "run_id": "r1",
                "status": "Success",
                "max_workers": 30,
                "duration_seconds": 600.0,
            },
        ]

        db = self._make_mock_db(runs=runs)

        def get_task_stats(run_id):
            if run_id == "r2":
                return {"total_duration": 8000.0, "task_count": 70, "avg_duration": 114.3}
            elif run_id == "r1":
                return {"total_duration": 9000.0, "task_count": 70, "avg_duration": 128.6}
            return None

        db.get_run_task_stats.side_effect = get_task_stats

        result = _detect_concurrency_ceiling(db, "wf")
        # No scale_up needed — already running at best level
        self.assertIsNone(result)


class TestCeilingIntegration(TestCase):
    """Tests for ceiling detection integrated into analyze_contention."""

    def _create_mock_stats_db(self, task_rows, ewma_durations, runs=None):
        """Create mock with both contention driver AND ceiling data."""
        stats_db = Mock()
        stats_db.enabled = True

        # Mock _conn.cursor() for _get_task_parameters
        mock_cursor = Mock()
        mock_rows = []
        for row in task_rows:
            mock_row = {
                "task_id": row["task_id"],
                "task_signature": row["task_signature"],
                "process": row["process"],
                "parameters": json.dumps(row["parameters"]),
            }
            mock_rows.append(mock_row)
        mock_cursor.fetchall.return_value = mock_rows
        stats_db._conn.cursor.return_value = mock_cursor

        # Mock get_workflow_signatures
        stats_db.get_workflow_signatures.return_value = list(ewma_durations.keys())

        # Mock get_task_durations
        def get_durations(sig, limit=10):
            return ewma_durations.get(sig, [])

        stats_db.get_task_durations.side_effect = get_durations

        # Mock ceiling-detection methods
        stats_db.get_runs_for_workflow.return_value = runs or []
        stats_db.get_run_task_stats.return_value = None
        stats_db.get_concurrent_task_counts.return_value = []

        return stats_db

    def test_ceiling_standalone_no_driver(self):
        """When no contention driver but ceiling found → result has ceiling recommendation."""
        # All tasks have same parameters → no varying keys → no driver
        # But we mock ceiling detection to find something
        task_rows = []
        ewma_durations = {}
        for i in range(10):
            sig = f"sig_{i}"
            task_rows.append(
                {
                    "task_id": str(i),
                    "task_signature": sig,
                    "process": "TestProcess",
                    "parameters": {"pYear": "2025"},  # All identical
                }
            )
            ewma_durations[sig] = [100.0 + i * 5]

        runs = [
            {
                "run_id": "r1",
                "status": "Success",
                "max_workers": 50,
                "duration_seconds": 700.0,
            }
        ]

        stats_db = self._create_mock_stats_db(task_rows, ewma_durations, runs=runs)

        # Mock ceiling data: strong correlation, low efficiency
        concurrent_data = [
            {
                "task_signature": f"sig_{i}",
                "duration_seconds": 100.0 + i * 30,
                "concurrent_count": 20 + i * 3,
            }
            for i in range(10)
        ]
        stats_db.get_concurrent_task_counts.return_value = concurrent_data
        stats_db.get_run_task_stats.return_value = {
            "total_duration": 15000.0,
            "task_count": 10,
            "avg_duration": 1500.0,
        }

        result = analyze_contention(stats_db, "wf")

        # No contention driver (all params identical)
        self.assertIsNone(result.contention_driver)
        # But ceiling should be detected
        self.assertIsNotNone(result.concurrency_ceiling)
        self.assertGreater(result.recommended_workers, 0)
        self.assertLess(result.recommended_workers, 50)

    def test_ceiling_caps_driver_recommendation(self):
        """When both driver and ceiling found, ceiling caps recommended_workers."""
        # KDP-like workflow with 10 segments × 3 periods
        task_rows = []
        ewma_durations = {}
        for seg_idx in range(10):
            for per_idx in range(3):
                task_id = str(seg_idx * 3 + per_idx)
                sig = f"sig_{seg_idx}_{per_idx}"
                # S8, S9 are heavy
                dur = 200.0 if seg_idx >= 8 else 15.0
                task_rows.append(
                    {
                        "task_id": task_id,
                        "task_signature": sig,
                        "process": "TestProcess",
                        "parameters": {"pSegment": f"S{seg_idx}", "pPeriod": f"P{per_idx}"},
                    }
                )
                ewma_durations[sig] = [dur]

        # Multi-run: 50w was slower than 20w
        runs = [
            {"run_id": "r2", "status": "Success", "max_workers": 20, "duration_seconds": 300.0},
            {"run_id": "r1", "status": "Success", "max_workers": 50, "duration_seconds": 500.0},
        ]

        stats_db = self._create_mock_stats_db(task_rows, ewma_durations, runs=runs)

        def get_task_stats(run_id):
            if run_id == "r2":
                return {"total_duration": 3000.0, "task_count": 30, "avg_duration": 100.0}
            elif run_id == "r1":
                return {"total_duration": 8000.0, "task_count": 30, "avg_duration": 266.7}
            return None

        stats_db.get_run_task_stats.side_effect = get_task_stats

        result = analyze_contention(stats_db, "wf", sensitivity=1.5)

        # Should have contention driver
        self.assertEqual(result.contention_driver, "pSegment")
        # Should also have ceiling
        self.assertIsNotNone(result.concurrency_ceiling)
        # Ceiling should cap the recommendation
        self.assertLessEqual(result.recommended_workers, result.concurrency_ceiling)

    def test_no_ceiling_no_driver(self):
        """When neither driver nor ceiling → empty result with warnings."""
        task_rows = []
        ewma_durations = {}
        for i in range(5):
            sig = f"sig_{i}"
            task_rows.append(
                {
                    "task_id": str(i),
                    "task_signature": sig,
                    "process": "TestProcess",
                    "parameters": {"pYear": "2025"},
                }
            )
            ewma_durations[sig] = [50.0]

        stats_db = self._create_mock_stats_db(task_rows, ewma_durations)

        result = analyze_contention(stats_db, "wf")

        self.assertIsNone(result.contention_driver)
        self.assertIsNone(result.concurrency_ceiling)
        self.assertTrue(len(result.warnings) > 0)
