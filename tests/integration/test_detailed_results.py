"""Integration test for ``--detailed-results``.

Exercises the full data path end-to-end without requiring a live TM1 instance:

    stats DB rows  →  build_results_dataframe  →  branch on detailed_results
                                              →  assign_unique_task_ids OR
                                                 summarize_expanded_tasks
                                              →  upload_results_to_tm1 (CSV bytes)

The CSV bytes are what ``}rushti.load.results`` consumes against the cube,
so the schema and column order asserted here are the contract between the
rushti Python layer and the TM1 control objects.
"""

import csv
import io
import unittest
from unittest.mock import MagicMock, Mock

from rushti.tm1_integration import (
    assign_unique_task_ids,
    build_results_dataframe,
    summarize_expanded_tasks,
    upload_results_to_tm1,
)


def _stats_db_with_expansion():
    """Stats DB rows for: workflow [1, 2*(3-way), 3] — task 2 expanded to 3."""
    mock = Mock()
    mock.get_run_results.return_value = [
        {
            "task_id": "1",
            "instance": "tm1srv01",
            "process": "p_pre",
            "parameters": "{}",
            "status": "Success",
            "start_time": "2026-05-01T10:00:00",
            "end_time": "2026-05-01T10:00:05",
            "duration_seconds": 5.0,
            "retry_count": 0,
            "error_message": None,
            "predecessors": "",
            "stage": "extract",
            "safe_retry": False,
            "timeout": "",
            "cancel_at_timeout": False,
            "require_predecessor_success": False,
            "succeed_on_minor_errors": False,
        },
        # Three expansions of task 2.
        {
            "task_id": "2",
            "instance": "tm1srv01",
            "process": "p_load",
            "parameters": '{"pMonth":"Jan"}',
            "status": "Success",
            "start_time": "2026-05-01T10:01:00",
            "end_time": "2026-05-01T10:01:10",
            "duration_seconds": 10.0,
            "retry_count": 0,
            "error_message": None,
            "predecessors": "[1]",
            "stage": "transform",
            "safe_retry": False,
            "timeout": "",
            "cancel_at_timeout": False,
            "require_predecessor_success": False,
            "succeed_on_minor_errors": False,
        },
        {
            "task_id": "2",
            "instance": "tm1srv01",
            "process": "p_load",
            "parameters": '{"pMonth":"Feb"}',
            "status": "Fail",
            "start_time": "2026-05-01T10:01:00",
            "end_time": "2026-05-01T10:01:08",
            "duration_seconds": 8.0,
            "retry_count": 1,
            "error_message": "TI failed",
            "predecessors": "[1]",
            "stage": "transform",
            "safe_retry": False,
            "timeout": "",
            "cancel_at_timeout": False,
            "require_predecessor_success": False,
            "succeed_on_minor_errors": False,
        },
        {
            "task_id": "2",
            "instance": "tm1srv01",
            "process": "p_load",
            "parameters": '{"pMonth":"Mar"}',
            "status": "Success",
            "start_time": "2026-05-01T10:01:00",
            "end_time": "2026-05-01T10:01:12",
            "duration_seconds": 12.0,
            "retry_count": 0,
            "error_message": None,
            "predecessors": "[1]",
            "stage": "transform",
            "safe_retry": False,
            "timeout": "",
            "cancel_at_timeout": False,
            "require_predecessor_success": False,
            "succeed_on_minor_errors": False,
        },
        {
            "task_id": "3",
            "instance": "tm1srv01",
            "process": "p_post",
            "parameters": "{}",
            "status": "Success",
            "start_time": "2026-05-01T10:02:00",
            "end_time": "2026-05-01T10:02:03",
            "duration_seconds": 3.0,
            "retry_count": 0,
            "error_message": None,
            "predecessors": "[2]",
            "stage": "load",
            "safe_retry": False,
            "timeout": "",
            "cancel_at_timeout": False,
            "require_predecessor_success": False,
            "succeed_on_minor_errors": False,
        },
    ]
    return mock


def _captured_csv(mock_tm1):
    """Decode the bytes passed to tm1.files.create() into rows of dicts."""
    file_content = mock_tm1.files.create.call_args.kwargs["file_content"]
    reader = csv.DictReader(io.StringIO(file_content.decode("utf-8")))
    return list(reader)


class TestDetailedResultsFalseSummarizes(unittest.TestCase):
    """Default path (--detailed-results not set) collapses expansions."""

    def test_three_rows_in_cube(self):
        stats_db = _stats_db_with_expansion()
        df = build_results_dataframe(stats_db, "wf", "run1")

        df = summarize_expanded_tasks(df)

        mock_tm1 = MagicMock()
        upload_results_to_tm1(mock_tm1, "wf", "run1", df)

        rows = _captured_csv(mock_tm1)

        # Three rows: tasks 1, 2 (collapsed), 3.
        self.assertEqual(len(rows), 3)
        task_ids = [r["task_id"] for r in rows]
        self.assertEqual(task_ids, ["1", "2", "3"])
        # Task 2 is the expansion summary — partial because one of three failed.
        summary = next(r for r in rows if r["task_id"] == "2")
        self.assertIn("Partial", summary["status"])
        # Parameters cell renders the expansion summary as inline strings
        # joined with "; " (one inline group per expansion).
        self.assertEqual(
            summary["parameters"],
            'pMonth="Jan"; pMonth="Feb"; pMonth="Mar"',
        )
        # original_task_id present for every row, equal to task_id (no renumbering).
        for row in rows:
            self.assertEqual(row["original_task_id"], row["task_id"])


class TestDetailedResultsTrueRenumbers(unittest.TestCase):
    """--detailed-results path emits one row per executed TI with gap-free IDs."""

    def test_five_rows_with_gap_free_ids(self):
        stats_db = _stats_db_with_expansion()
        df = build_results_dataframe(stats_db, "wf", "run1")

        df = assign_unique_task_ids(df)

        mock_tm1 = MagicMock()
        upload_results_to_tm1(mock_tm1, "wf", "run1", df)

        rows = _captured_csv(mock_tm1)

        # Five rows, gap-free renumber: 1, 2, 3, 4, 5.
        self.assertEqual(len(rows), 5)
        self.assertEqual([r["task_id"] for r in rows], ["1", "2", "3", "4", "5"])

        # original_task_id reflects pre-renumber identity.
        # 1 → 1, three expansions of 2 → 2, 3 → 3.
        self.assertEqual(
            [r["original_task_id"] for r in rows],
            ["1", "2", "2", "2", "3"],
        )

        # Parameters column is rendered inline (matching the input cube
        # format), not as JSON dicts. Empty params render as empty string.
        self.assertEqual(
            [r["parameters"] for r in rows],
            ["", 'pMonth="Jan"', 'pMonth="Feb"', 'pMonth="Mar"', ""],
        )

        # predecessors column references original task IDs, not renumbered ones.
        # Row 5 (was task 3) had predecessors=[2] in the workflow definition;
        # that string passes through verbatim. Documented join contract:
        # downstream tasks reconcile predecessors against original_task_id.
        row5 = rows[4]
        self.assertEqual(row5["original_task_id"], "3")
        self.assertEqual(row5["predecessors"], "[2]")

        # Each expansion has its own status and duration in the cube.
        expansion_statuses = [r["status"] for r in rows if r["original_task_id"] == "2"]
        self.assertIn("Success", expansion_statuses)
        self.assertIn("Fail", expansion_statuses)

    def test_csv_header_includes_workflow_run_id_original_task_id(self):
        stats_db = _stats_db_with_expansion()
        df = build_results_dataframe(stats_db, "wf", "run1")
        df = assign_unique_task_ids(df)

        mock_tm1 = MagicMock()
        upload_results_to_tm1(mock_tm1, "wf", "run1", df)

        file_content = mock_tm1.files.create.call_args.kwargs["file_content"]
        reader = csv.reader(io.StringIO(file_content.decode("utf-8")))
        header = next(reader)
        # Contract with }rushti.load.results: column order matters because the
        # TI reads CSV columns positionally and binds them to its variables.
        self.assertEqual(header[0], "workflow")
        self.assertEqual(header[1], "run_id")
        self.assertEqual(header[2], "task_id")
        self.assertEqual(header[3], "original_task_id")


if __name__ == "__main__":
    unittest.main()
