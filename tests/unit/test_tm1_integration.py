"""Unit tests for TM1 integration module.

Tests the tm1_integration module which provides functionality for reading
task definitions from TM1 cubes and writing execution results.
"""

import unittest
from unittest.mock import Mock, MagicMock, patch

import pandas as pd

from rushti.tm1_integration import (
    INPUT_MEASURES,
    _dataframe_to_task_definitions,
    _parse_bool,
    _parse_parameters_string,
    assign_unique_task_ids,
    build_results_dataframe,
    connect_to_tm1_instance,
    read_taskfile_from_tm1,
)

# Default cube name for testing
DEFAULT_CUBE_NAME = "rushti"


class TestParseBool(unittest.TestCase):
    """Tests for _parse_bool helper function."""

    def test_parse_bool_true_values(self):
        """Test parsing various true values."""
        true_values = [True, 1, 1.0, "true", "True", "TRUE", "1", "yes", "t"]

        for value in true_values:
            with self.subTest(value=value):
                self.assertTrue(_parse_bool(value))

    def test_parse_bool_false_values(self):
        """Test parsing various false values."""
        false_values = [False, 0, 0.0, "false", "False", "FALSE", "0", "no", ""]

        for value in false_values:
            with self.subTest(value=value):
                self.assertFalse(_parse_bool(value))


class TestParseParametersString(unittest.TestCase):
    """Tests for _parse_parameters_string helper function."""

    def test_json_format(self):
        """Test parsing JSON format parameters."""
        result = _parse_parameters_string('{"pWaitSec": "1", "pLogOutput": "Yes"}')
        self.assertEqual(result, {"pWaitSec": "1", "pLogOutput": "Yes"})

    def test_json_empty_object(self):
        """Test parsing empty JSON object."""
        result = _parse_parameters_string("{}")
        self.assertEqual(result, {})

    def test_space_separated_simple(self):
        """Test parsing simple space-separated key=value pairs."""
        result = _parse_parameters_string("pWaitSec=1 pLogOutput=Yes")
        self.assertEqual(result, {"pWaitSec": "1", "pLogOutput": "Yes"})

    def test_space_separated_quoted_values(self):
        """Test parsing space-separated with quoted values."""
        result = _parse_parameters_string('pSourceDim="Employee" pTargetDim="Employee Backup"')
        self.assertEqual(result, {"pSourceDim": "Employee", "pTargetDim": "Employee Backup"})

    def test_space_separated_single_param(self):
        """Test parsing a single space-separated parameter."""
        result = _parse_parameters_string("pWaitSec=5")
        self.assertEqual(result, {"pWaitSec": "5"})

    def test_empty_string(self):
        """Test parsing empty string."""
        result = _parse_parameters_string("")
        self.assertEqual(result, {})

    def test_whitespace_only(self):
        """Test parsing whitespace-only string."""
        result = _parse_parameters_string("   ")
        self.assertEqual(result, {})

    def test_no_equals_sign(self):
        """Test parsing string with no key=value pairs."""
        result = _parse_parameters_string("not valid json")
        self.assertEqual(result, {})

    def test_json_non_dict_returns_empty(self):
        """Test that JSON arrays are not treated as valid parameters."""
        result = _parse_parameters_string('["a", "b"]')
        # Falls through to shlex parsing, no key=value found
        self.assertEqual(result, {})

    def test_space_separated_with_equals_in_value(self):
        """Test parsing where value contains equals sign."""
        result = _parse_parameters_string('pFilter="[dim].[hier].[elem]=value"')
        self.assertEqual(result, {"pFilter": "[dim].[hier].[elem]=value"})

    def test_mixed_numeric_and_string_values(self):
        """Test parsing mixed numeric and string values."""
        result = _parse_parameters_string("pYear=2024 pMonth=January pDebug=1")
        self.assertEqual(result, {"pYear": "2024", "pMonth": "January", "pDebug": "1"})

    def test_unmatched_quotes_returns_empty(self):
        """Test that unmatched quotes return empty dict gracefully."""
        result = _parse_parameters_string('pBad="unclosed')
        self.assertEqual(result, {})


class TestConnectToTM1Instance(unittest.TestCase):
    """Tests for connect_to_tm1_instance function."""

    @patch("rushti.tm1_integration.configparser.ConfigParser")
    @patch("rushti.tm1_integration.TM1Service")
    def test_connect_success(self, mock_tm1_service, mock_config_parser):
        """Test successful connection to TM1 instance."""
        # Setup mock config
        mock_config = MagicMock()
        mock_config.sections.return_value = ["tm1srv01"]
        mock_config.__getitem__.return_value = {
            "address": "localhost",
            "port": "12345",
            "user": "admin",
            "password": "apple",
        }
        mock_config.getboolean.return_value = False
        mock_config.get.return_value = None
        mock_config_parser.return_value = mock_config

        # Setup mock TM1Service
        mock_tm1 = Mock()
        mock_tm1_service.return_value = mock_tm1

        result = connect_to_tm1_instance("tm1srv01", "config.ini")

        self.assertEqual(result, mock_tm1)
        mock_config.read.assert_called_once()
        mock_tm1_service.assert_called_once()

    @patch("rushti.tm1_integration.configparser.ConfigParser")
    def test_connect_instance_not_found(self, mock_config_parser):
        """Test connection when instance not in config."""
        mock_config = Mock()
        mock_config.sections.return_value = ["tm1srv01"]
        mock_config_parser.return_value = mock_config

        with self.assertRaises(ValueError) as ctx:
            connect_to_tm1_instance("nonexistent", "config.ini")

        self.assertIn("not found", str(ctx.exception))

    def test_connect_no_config_path(self):
        """Test connection with no config path provided."""
        with self.assertRaises(ValueError) as ctx:
            connect_to_tm1_instance("tm1srv01", "")

        self.assertIn("required", str(ctx.exception))

    @patch("rushti.tm1_integration.configparser.ConfigParser")
    @patch("rushti.tm1_integration.TM1Service")
    def test_connect_connection_error(self, mock_tm1_service, mock_config_parser):
        """Test connection failure."""
        mock_config = MagicMock()
        mock_config.sections.return_value = ["tm1srv01"]
        mock_config.__getitem__.return_value = {}
        mock_config_parser.return_value = mock_config

        mock_tm1_service.side_effect = Exception("Connection failed")

        with self.assertRaises(ConnectionError) as ctx:
            connect_to_tm1_instance("tm1srv01", "config.ini")

        self.assertIn("Failed to connect", str(ctx.exception))


class TestDataFrameToTaskDefinitions(unittest.TestCase):
    """Tests for _dataframe_to_task_definitions function."""

    def test_convert_norm_mode(self):
        """Test converting DataFrame to tasks in norm mode."""
        # Create sample DataFrame
        data = {
            "rushti_task_id": ["1", "2"],
            "instance": ["tm1srv01", "tm1srv01"],
            "process": ["proc1", "proc2"],
            "parameters": ['{"p1": "v1"}', '{"p2": "v2"}'],
            "predecessors": ["", "1"],
            "stage": ["stage1", "stage2"],
            "safe_retry": [False, True],
            "timeout": ["60", ""],
            "cancel_at_timeout": [False, False],
            "require_predecessor_success": [True, True],
            "succeed_on_minor_errors": [False, False],
            "wait": [False, False],
        }
        df = pd.DataFrame(data)

        tasks = _dataframe_to_task_definitions(df, mode="norm")

        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].id, "1")
        self.assertEqual(tasks[0].process, "proc1")
        self.assertEqual(tasks[0].parameters, {"p1": "v1"})
        self.assertEqual(tasks[1].safe_retry, True)
        self.assertEqual(tasks[0].timeout, 60)
        self.assertIsNone(tasks[1].timeout)

    def test_convert_opt_mode(self):
        """Test converting DataFrame to tasks in opt mode."""
        data = {
            "rushti_task_id": ["1", "2", "3"],
            "instance": ["tm1srv01", "tm1srv01", "tm1srv01"],
            "process": ["proc1", "proc2", "proc3"],
            "parameters": ["{}", "{}", "{}"],
            "predecessors": ["", "1", "1,2"],
            "stage": ["", "", ""],
            "safe_retry": [False, False, False],
            "timeout": ["", "", ""],
            "cancel_at_timeout": [False, False, False],
            "require_predecessor_success": [True, True, True],
            "succeed_on_minor_errors": [False, False, False],
            "wait": [False, False, False],
        }
        df = pd.DataFrame(data)

        tasks = _dataframe_to_task_definitions(df, mode="opt")

        self.assertEqual(len(tasks), 3)
        self.assertEqual(tasks[0].predecessors, [])
        self.assertEqual(tasks[1].predecessors, ["1"])
        self.assertEqual(tasks[2].predecessors, ["1", "2"])

    def test_convert_with_wait_markers(self):
        """Test converting DataFrame with wait markers in norm mode."""
        data = {
            "rushti_task_id": ["1", "wait", "2"],
            "instance": ["tm1srv01", "", "tm1srv01"],
            "process": ["proc1", "", "proc2"],
            "parameters": ["{}", "", "{}"],
            "predecessors": ["", "", ""],
            "stage": ["", "", ""],
            "safe_retry": [False, False, False],
            "timeout": ["", "", ""],
            "cancel_at_timeout": [False, False, False],
            "require_predecessor_success": [True, True, True],
            "succeed_on_minor_errors": [False, False, False],
            "wait": [False, True, False],
        }
        df = pd.DataFrame(data)

        tasks = _dataframe_to_task_definitions(df, mode="norm")

        # Should have 2 tasks (wait marker skipped)
        self.assertEqual(len(tasks), 2)
        # Task 2 should have task 1 as predecessor (due to wait)
        self.assertEqual(tasks[1].predecessors, ["1"])

    def test_convert_skip_empty_rows(self):
        """Test that empty rows are skipped."""
        data = {
            "rushti_task_id": ["1", "2", "3"],
            "instance": ["tm1srv01", "", "tm1srv01"],
            "process": ["proc1", "", "proc3"],
            "parameters": ["{}", "", "{}"],
            "predecessors": ["", "", ""],
            "stage": ["", "", ""],
            "safe_retry": [False, False, False],
            "timeout": ["", "", ""],
            "cancel_at_timeout": [False, False, False],
            "require_predecessor_success": [True, True, True],
            "succeed_on_minor_errors": [False, False, False],
            "wait": [False, False, False],
        }
        df = pd.DataFrame(data)

        tasks = _dataframe_to_task_definitions(df, mode="norm")

        # Row 2 should be skipped (no instance/process)
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].id, "1")
        self.assertEqual(tasks[1].id, "3")

    def test_convert_unparseable_parameters(self):
        """Test handling of completely unparseable parameters."""
        data = {
            "rushti_task_id": ["1"],
            "instance": ["tm1srv01"],
            "process": ["proc1"],
            "parameters": ["not valid json"],
            "predecessors": [""],
            "stage": [""],
            "safe_retry": [False],
            "timeout": [""],
            "cancel_at_timeout": [False],
            "require_predecessor_success": [True],
            "succeed_on_minor_errors": [False],
            "wait": [False],
        }
        df = pd.DataFrame(data)

        tasks = _dataframe_to_task_definitions(df, mode="norm")

        # No '=' in the string, so no key=value pairs found
        self.assertEqual(tasks[0].parameters, {})

    def test_convert_space_separated_parameters(self):
        """Test parsing space-separated parameters from TM1 cube."""
        data = {
            "rushti_task_id": ["1", "2"],
            "instance": ["tm1srv01", "tm1srv01"],
            "process": ["proc1", "proc2"],
            "parameters": ['pWaitSec=1 pLogOutput="Yes"', "pYear=2024"],
            "predecessors": ["", "1"],
            "stage": ["", ""],
            "safe_retry": [False, False],
            "timeout": ["", ""],
            "cancel_at_timeout": [False, False],
            "require_predecessor_success": [True, True],
            "succeed_on_minor_errors": [False, False],
            "wait": [False, False],
        }
        df = pd.DataFrame(data)

        tasks = _dataframe_to_task_definitions(df, mode="opt")

        self.assertEqual(tasks[0].parameters, {"pWaitSec": "1", "pLogOutput": "Yes"})
        self.assertEqual(tasks[1].parameters, {"pYear": "2024"})


class TestReadTaskfileFromTM1(unittest.TestCase):
    """Tests for read_taskfile_from_tm1 function."""

    @patch("rushti.tm1_integration._dataframe_to_task_definitions")
    def test_read_taskfile_success(self, mock_df_to_tasks):
        """Test successfully reading taskfile from TM1."""
        # Create mock TM1 service
        mock_tm1 = Mock()

        # Create sample DataFrame
        df = pd.DataFrame({"rushti_task_id": ["1"], "instance": ["tm1srv01"], "process": ["proc1"]})
        mock_tm1.cells.execute_mdx_dataframe_shaped.return_value = df

        # Mock task definitions
        from rushti.taskfile import TaskDefinition

        mock_tasks = [TaskDefinition(id="1", instance="tm1srv01", process="proc1", parameters={})]
        mock_df_to_tasks.return_value = mock_tasks

        taskfile = read_taskfile_from_tm1(mock_tm1, "test-taskfile")

        self.assertEqual(taskfile.metadata.workflow, "test-taskfile")
        self.assertEqual(len(taskfile.tasks), 1)
        self.assertEqual(taskfile.tasks[0].id, "1")

    def test_read_taskfile_empty_result(self):
        """Test reading taskfile with empty result."""
        mock_tm1 = Mock()
        mock_tm1.cells.execute_mdx_dataframe_shaped.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as ctx:
            read_taskfile_from_tm1(mock_tm1, "empty-taskfile")

        self.assertIn("No tasks found", str(ctx.exception))

    def test_read_taskfile_query_error(self):
        """Test reading taskfile when MDX query fails."""
        mock_tm1 = Mock()
        mock_tm1.cells.execute_mdx_dataframe_shaped.side_effect = Exception("Query failed")

        with self.assertRaises(ValueError) as ctx:
            read_taskfile_from_tm1(mock_tm1, "error-taskfile")

        self.assertIn("Failed to read workflow", str(ctx.exception))


class TestBuildResultsDataFrame(unittest.TestCase):
    """Tests for build_results_dataframe function."""

    def test_build_results_dataframe(self):
        """Test building results DataFrame from stats database."""
        # Mock stats database
        mock_stats_db = Mock()
        mock_stats_db.get_run_results.return_value = [
            {
                "task_id": "1",
                "instance": "tm1srv01",
                "process": "proc1",
                "parameters": "{}",
                "status": "Success",
                "start_time": "2024-01-01T10:00:00",
                "end_time": "2024-01-01T10:00:05",
                "duration_seconds": 5.0,
                "retry_count": 0,
                "error_message": None,
                "predecessors": "",
                "stage": "",
                "safe_retry": False,
                "timeout": "",
                "cancel_at_timeout": False,
                "require_predecessor_success": True,
                "succeed_on_minor_errors": False,
            }
        ]

        df = build_results_dataframe(mock_stats_db, "taskfile1", "run1")

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["task_id"], "1")
        self.assertEqual(df.iloc[0]["status"], "Success")
        self.assertEqual(df.iloc[0]["duration_seconds"], 5.0)
        # original_task_id is populated unconditionally; equals task_id pre-renumber.
        self.assertIn("original_task_id", df.columns)
        self.assertEqual(df.iloc[0]["original_task_id"], "1")

    def test_build_results_dataframe_original_task_id_for_expansions(self):
        """All expansions of one original task share the same original_task_id."""
        mock_stats_db = Mock()
        mock_stats_db.get_run_results.return_value = [
            {"task_id": "2", "instance": "tm1srv01", "process": "p", "status": "Success"},
            {"task_id": "2", "instance": "tm1srv01", "process": "p", "status": "Success"},
            {"task_id": "2", "instance": "tm1srv01", "process": "p", "status": "Fail"},
        ]
        df = build_results_dataframe(mock_stats_db, "wf", "run1")
        self.assertEqual(len(df), 3)
        # All three rows share the same original_task_id (= task_id pre-renumber).
        self.assertTrue((df["original_task_id"] == "2").all())
        self.assertTrue((df["task_id"] == df["original_task_id"]).all())

    def test_build_results_dataframe_empty(self):
        """Test building results DataFrame with no results."""
        mock_stats_db = Mock()
        mock_stats_db.get_run_results.return_value = []

        df = build_results_dataframe(mock_stats_db, "taskfile1", "run1")

        self.assertTrue(df.empty)


class TestAssignUniqueTaskIds(unittest.TestCase):
    """Tests for assign_unique_task_ids — detailed-results renumbering."""

    @staticmethod
    def _frame(rows):
        """Build a minimal results DataFrame from (task_id, start_time, ...) tuples."""
        return pd.DataFrame(
            [
                {
                    "task_id": tid,
                    "original_task_id": tid,
                    "start_time": st,
                    "instance": "tm1srv01",
                    "process": "p",
                    "predecessors": preds,
                }
                for tid, st, preds in rows
            ]
        )

    def test_empty_dataframe_passes_through(self):
        df = pd.DataFrame()
        result = assign_unique_task_ids(df)
        self.assertTrue(result.empty)

    def test_no_expansions_renumbers_in_order(self):
        # Five distinct tasks, no duplicates → renumbered 1..5 in original order.
        df = self._frame(
            [
                ("1", "2026-05-01T10:00:00", ""),
                ("2", "2026-05-01T10:00:01", ""),
                ("3", "2026-05-01T10:00:02", ""),
                ("4", "2026-05-01T10:00:03", ""),
                ("5", "2026-05-01T10:00:04", ""),
            ]
        )
        result = assign_unique_task_ids(df)
        self.assertEqual(list(result["task_id"]), ["1", "2", "3", "4", "5"])
        # original_task_id preserved.
        self.assertEqual(list(result["original_task_id"]), ["1", "2", "3", "4", "5"])

    def test_single_expansion_gap_free(self):
        # Original [1, 2*(3), 3] → renumbered [1, 2, 3, 4, 5], gap-free.
        df = self._frame(
            [
                ("1", "2026-05-01T10:00:00", ""),
                ("2", "2026-05-01T10:01:00", ""),  # expansion 1
                ("2", "2026-05-01T10:01:01", ""),  # expansion 2
                ("2", "2026-05-01T10:01:02", ""),  # expansion 3
                ("3", "2026-05-01T10:02:00", ""),
            ]
        )
        result = assign_unique_task_ids(df)
        self.assertEqual(list(result["task_id"]), ["1", "2", "3", "4", "5"])
        # Three middle rows share the original task_id 2.
        expansions = result[result["task_id"].isin(["2", "3", "4"])]
        self.assertTrue((expansions["original_task_id"] == "2").all())

    def test_multiple_expansions_gap_free(self):
        # Two expandable tasks: [1, 2*(2), 3, 4*(3)] → [1, 2, 3, 4, 5, 6, 7].
        df = self._frame(
            [
                ("1", "2026-05-01T10:00:00", ""),
                ("2", "2026-05-01T10:01:00", ""),
                ("2", "2026-05-01T10:01:01", ""),
                ("3", "2026-05-01T10:02:00", ""),
                ("4", "2026-05-01T10:03:00", ""),
                ("4", "2026-05-01T10:03:01", ""),
                ("4", "2026-05-01T10:03:02", ""),
            ]
        )
        result = assign_unique_task_ids(df)
        self.assertEqual(list(result["task_id"]), ["1", "2", "3", "4", "5", "6", "7"])
        # original_task_id reflects pre-renumbering identity.
        self.assertEqual(
            list(result["original_task_id"]),
            ["1", "2", "2", "3", "4", "4", "4"],
        )

    def test_predecessors_untouched(self):
        # predecessors column must reference the original_task_id, not the
        # renumbered IDs. assign_unique_task_ids must not modify it.
        df = self._frame(
            [
                ("1", "2026-05-01T10:00:00", ""),
                ("2", "2026-05-01T10:01:00", "[1]"),
                ("2", "2026-05-01T10:01:01", "[1]"),
                ("3", "2026-05-01T10:02:00", "[2]"),
            ]
        )
        result = assign_unique_task_ids(df)
        self.assertEqual(list(result["task_id"]), ["1", "2", "3", "4"])
        # Original predecessors strings preserved verbatim.
        self.assertEqual(list(result["predecessors"]), ["", "[1]", "[1]", "[2]"])

    def test_sort_by_start_time_within_group(self):
        # Out-of-order start times → renumbering follows time order within a group.
        df = self._frame(
            [
                ("1", "2026-05-01T10:00:00", ""),
                ("2", "2026-05-01T10:01:05", ""),  # later
                ("2", "2026-05-01T10:01:00", ""),  # earlier
                ("2", "2026-05-01T10:01:02", ""),  # middle
            ]
        )
        result = assign_unique_task_ids(df)
        # Within the original-2 group: earliest start_time gets new ID 2,
        # then 3, then 4.
        group = result[result["original_task_id"] == "2"].reset_index(drop=True)
        self.assertEqual(list(group["task_id"]), ["2", "3", "4"])
        self.assertEqual(
            list(group["start_time"]),
            [
                "2026-05-01T10:01:00",
                "2026-05-01T10:01:02",
                "2026-05-01T10:01:05",
            ],
        )

    def test_deterministic_on_repeat(self):
        # Running twice on the same input produces identical output.
        df = self._frame(
            [
                ("1", "2026-05-01T10:00:00", ""),
                ("2", "2026-05-01T10:01:00", ""),
                ("2", "2026-05-01T10:01:00", ""),  # tie on start_time
                ("3", "2026-05-01T10:02:00", ""),
            ]
        )
        a = assign_unique_task_ids(df)
        b = assign_unique_task_ids(df)
        pd.testing.assert_frame_equal(a, b)

    def test_skipped_group_with_empty_start_time(self):
        # A whole-group skip — all expansions have empty start_time. They should
        # still receive sequential IDs (sort-last within their group).
        df = self._frame(
            [
                ("1", "2026-05-01T10:00:00", ""),
                ("2", "", ""),
                ("2", "", ""),
                ("3", "2026-05-01T10:02:00", ""),
            ]
        )
        result = assign_unique_task_ids(df)
        self.assertEqual(list(result["task_id"]), ["1", "2", "3", "4"])
        self.assertEqual(list(result["original_task_id"]), ["1", "2", "2", "3"])

    def test_missing_original_task_id_column_falls_back(self):
        # Defensive fallback: if a caller hands a frame without the column,
        # synthesize it from task_id rather than crash.
        df = pd.DataFrame(
            [
                {"task_id": "1", "start_time": "2026-05-01T10:00:00"},
                {"task_id": "2", "start_time": "2026-05-01T10:01:00"},
            ]
        )
        result = assign_unique_task_ids(df)
        self.assertEqual(list(result["task_id"]), ["1", "2"])
        self.assertEqual(list(result["original_task_id"]), ["1", "2"])

    def test_groups_iterated_in_numeric_order_not_lex(self):
        # Sort key is int(original_task_id). With original IDs 1, 2, 10:
        # iteration must be 1, 2, 10 — not lex 1, 10, 2.
        df = self._frame(
            [
                ("10", "2026-05-01T10:00:02", ""),
                ("2", "2026-05-01T10:00:01", ""),
                ("1", "2026-05-01T10:00:00", ""),
            ]
        )
        result = assign_unique_task_ids(df)
        self.assertEqual(list(result["original_task_id"]), ["1", "2", "10"])
        self.assertEqual(list(result["task_id"]), ["1", "2", "3"])


class TestConstants(unittest.TestCase):
    """Tests for module constants."""

    def test_default_cube_name(self):
        """Test default cube name constant."""
        self.assertEqual(DEFAULT_CUBE_NAME, "rushti")

    def test_input_measures(self):
        """Test that all required input measures are defined."""
        required = [
            "instance",
            "process",
            "parameters",
            "predecessors",
            "stage",
            "safe_retry",
            "timeout",
            "cancel_at_timeout",
            "require_predecessor_success",
            "succeed_on_minor_errors",
            "wait",
        ]

        for measure in required:
            self.assertIn(measure, INPUT_MEASURES)


if __name__ == "__main__":
    unittest.main()
