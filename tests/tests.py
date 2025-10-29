import unittest

from rushti import (
    deduce_levels_of_tasks,
    extract_tasks_from_file_type_opt,
    extract_ordered_tasks_and_waits_from_file_type_opt,
    parse_line_arguments,
)
from utils import OptimizedTask, Wait


class TestDataMethods(unittest.TestCase):
    def test_deduce_levels_of_tasks_case1(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case1.txt")
        expected_outcome = {
            0: ["1"],
            1: ["2"],
            2: ["3"],
            3: ["4"],
            4: ["5"],
            5: ["6"],
            6: ["7"],
            7: ["8"],
            8: ["9"],
        }
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case2(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case2.txt")
        expected_outcome = {
            0: ["1"],
            1: ["2", "3", "4", "5"],
            2: ["6", "7", "8", "9"],
            3: ["10", "11", "12", "13"],
            4: ["14", "15", "16", "17"],
        }
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case3(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case3.txt")
        expected_outcome = {
            0: ["1"],
            1: ["2", "3", "4", "9"],
            2: ["5", "7"],
            3: ["6"],
            4: ["8"],
        }
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case4(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case4.txt")
        expected_outcome = {
            8: ["8"],
            7: ["7"],
            6: ["6"],
            5: ["5"],
            4: ["4"],
            3: ["3"],
            2: ["2"],
            1: ["1"],
            0: ["9"],
        }
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case5(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case5.txt")
        expected_outcome = {0: ["11", "12", "21", "22"], 1: ["13", "23"]}
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case6(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case6.txt")
        expected_outcome = {
            0: ["1", "2"],
            1: ["3"],
            2: ["5"],
            3: ["6", "7"],
        }
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_extract_lines_from_file_type_opt_happy_case(self):
        ordered_tasks = extract_ordered_tasks_and_waits_from_file_type_opt(
            5, r"tests/resources/tasks_opt_happy_case.txt"
        )

        expected_tasks = [
            OptimizedTask(
                "1", "tm1srv01", "}bedrock.server.wait", {"pWaitSec": "1"}, [], True
            ),
            Wait(),
            OptimizedTask(
                "2", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "2"}, ["1"], True
            ),
            Wait(),
            OptimizedTask(
                "3", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "2"}, ["2"], True
            ),
            Wait(),
            OptimizedTask(
                "4", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "2"}, ["3"], True
            ),
            Wait(),
        ]

        for expected_task, ordered_task in zip(expected_tasks, ordered_tasks):
            self.assertIsInstance(ordered_task, type(expected_task))

            if isinstance(expected_task, OptimizedTask):
                self.assertEqual(expected_task.id, ordered_task.id)
                self.assertEqual(
                    expected_task.instance_name, ordered_task.instance_name
                )
                self.assertEqual(expected_task.process_name, ordered_task.process_name)
                self.assertEqual(expected_task.parameters, ordered_task.parameters)
                self.assertEqual(expected_task.predecessors, ordered_task.predecessors)
                self.assertEqual(
                    expected_task.require_predecessor_success,
                    ordered_task.require_predecessor_success,
                )

    def test_extract_lines_from_file_type_opt_multi_task_per_id(self):
        ordered_tasks = extract_ordered_tasks_and_waits_from_file_type_opt(
            5, r"tests/resources/tasks_opt_multi_task_per_id.txt"
        )

        expected_tasks = [
            OptimizedTask(
                "1", "tm1srv01", "}bedrock.server.wait", {"pWaitSec": "1"}, [], True
            ),
            Wait(),
            OptimizedTask(
                "2", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "2"}, ["1"], True
            ),
            OptimizedTask(
                "2", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "3"}, ["1"], True
            ),
            Wait(),
            OptimizedTask(
                "3", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "4"}, ["2"], True
            ),
            Wait(),
        ]

        for expected_task, ordered_task in zip(expected_tasks, ordered_tasks):
            self.assertIsInstance(ordered_task, type(expected_task))

            if isinstance(expected_task, OptimizedTask):
                self.assertEqual(expected_task.id, ordered_task.id)
                self.assertEqual(
                    expected_task.instance_name, ordered_task.instance_name
                )
                self.assertEqual(expected_task.process_name, ordered_task.process_name)
                self.assertEqual(expected_task.parameters, ordered_task.parameters)
                self.assertEqual(expected_task.predecessors, ordered_task.predecessors)
                self.assertEqual(
                    expected_task.require_predecessor_success,
                    ordered_task.require_predecessor_success,
                )


class TestParseLineArguments(unittest.TestCase):
    def test_basic_arguments(self):
        line = 'instance=tm1 process=process1 param1="value1" param2="value 2"'
        result = parse_line_arguments(line)
        expected = {
            "instance": "tm1",
            "process": "process1",
            "param1": "value1",
            "param2": "value 2",
        }
        self.assertEqual(result, expected)

    def test_nested_double_quotes(self):
        line = 'instance=tm1 process=process1 param1="value with \\"quotes\\"" param2="simple"'
        result = parse_line_arguments(line)
        expected = {
            "instance": "tm1",
            "process": "process1",
            "param1": 'value with "quotes"',
            "param2": "simple",
        }
        self.assertEqual(result, expected)

    def test_backslashes(self):
        line = r'instance=tm1 process=process1 param1="value\\with\\backslashes" param2="normal"'
        result = parse_line_arguments(line)
        expected = {
            "instance": "tm1",
            "process": "process1",
            "param1": r"value\with\backslashes",
            "param2": "normal",
        }
        self.assertEqual(result, expected)

    def test_complex_nested_quotes(self):
        line = r'instance=tm1 process=process1 param1="outer \"inner \\\"deepest\\\" inner\" outer"'
        result = parse_line_arguments(line)
        expected = {
            "instance": "tm1",
            "process": "process1",
            "param1": 'outer "inner \\"deepest\\" inner" outer',
        }
        self.assertEqual(result, expected)

    def test_predecessors_and_require_predecessor_success(self):
        line = 'id=1 instance=tm1 process=process1 predecessors="2,3,4" require_predecessor_success="true"'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "instance": "tm1",
            "process": "process1",
            "predecessors": ["2", "3", "4"],
            "require_predecessor_success": True,
        }
        self.assertEqual(result, expected)

    def test_sql_query_parsing(self):
        self.maxDiff = None
        line = 'id="1" predecessors="" require_predecessor_success="" instance="tm1srv01" process="}bedrock.server.query" pQuery="SELECT Id,IsDeleted FROM Account WHERE date=\\"20241031092120\\"" pParam2="" pParam3="testing\\"2\\""'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "predecessors": [],
            "require_predecessor_success": False,
            "instance": "tm1srv01",
            "process": "}bedrock.server.query",
            "pQuery": 'SELECT Id,IsDeleted FROM Account WHERE date="20241031092120"',
            "pParam2": "",
            "pParam3": 'testing"2"',
        }
        self.assertEqual(result, expected)


class TestSucceedOnMinorErrors(unittest.TestCase):
    def test_default_value(self):
        line = 'id=1 instance=tm1 process=process1 predecessors="2,3,4" require_predecessor_success="1"'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "instance": "tm1",
            "process": "process1",
            "predecessors": ["2", "3", "4"],
            "require_predecessor_success": True,
        }
        self.assertEqual(result, expected)

    def test_explicit_false_value(self):
        line = 'id=1 instance=tm1 process=process1 predecessors="2,3,4" require_predecessor_success="1" succeed_on_minor_errors="0"'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "instance": "tm1",
            "process": "process1",
            "predecessors": ["2", "3", "4"],
            "require_predecessor_success": True,
            "succeed_on_minor_errors": False,
        }
        self.assertEqual(result, expected)

    def test_explicit_true_value(self):
        line = 'id=1 instance=tm1 process=process1 predecessors="" require_predecessor_success="" succeed_on_minor_errors="1"'
        result = parse_line_arguments(line)
        expected = {
            "id": "1",
            "instance": "tm1",
            "process": "process1",
            "predecessors": [],
            "require_predecessor_success": False,
            "succeed_on_minor_errors": True,
        }
        self.assertEqual(result, expected)

    def test_line_translation_with_succeed_on_minor_errors(self):
        task = OptimizedTask(
            "1",
            "tm1srv01",
            "process1",
            {"param1": "value1"},
            [],
            False,
            succeed_on_minor_errors=True,
        )
        expected_line = 'id="1" predecessors="" require_predecessor_success="False" succeed_on_minor_errors="True" instance="tm1srv01" process="process1" param1="value1"\n'
        self.assertEqual(task.translate_to_line(), expected_line)

    def test_line_translation_without_succeed_on_minor_errors(self):
        task = OptimizedTask(
            "1", "tm1srv01", "process1", {"param1": "value1"}, [2, 3, 4], False
        )
        expected_line = 'id="1" predecessors="2,3,4" require_predecessor_success="False" succeed_on_minor_errors="False" instance="tm1srv01" process="process1" param1="value1"\n'
        self.assertEqual(task.translate_to_line(), expected_line)


if __name__ == '__main__':
    unittest.main()
