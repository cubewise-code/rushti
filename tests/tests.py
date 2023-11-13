import unittest

from rushti import deduce_levels_of_tasks, extract_tasks_from_file_type_opt, \
    extract_ordered_tasks_and_waits_from_file_type_opt
from utils import OptimizedTask, Wait


class TestDataMethods(unittest.TestCase):

    def test_deduce_levels_of_tasks_case1(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case1.txt")
        expected_outcome = {
            0: ['1'],
            1: ['2'],
            2: ['3'],
            3: ['4'],
            4: ['5'],
            5: ['6'],
            6: ['7'],
            7: ['8'],
            8: ['9']}
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case2(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case2.txt")
        expected_outcome = {
            0: ['1'],
            1: ['2', '3', '4', '5'],
            2: ['6', '7', '8', '9'],
            3: ['10', '11', '12', '13'],
            4: ['14', '15', '16', '17']}
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case3(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case3.txt")
        expected_outcome = {
            0: ['1'],
            1: ['2', '3', '4', '9'],
            2: ['5', '7'],
            3: ['6'],
            4: ['8']}
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case4(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case4.txt")
        expected_outcome = {
            8: ['8'],
            7: ['7'],
            6: ['6'],
            5: ['5'],
            4: ['4'],
            3: ['3'],
            2: ['2'],
            1: ['1'],
            0: ['9']}
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case5(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case5.txt")
        expected_outcome = {
            0: ['11', '12', '21', '22'],
            1: ['13', '23']}
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_deduce_levels_of_tasks_case6(self):
        tasks = extract_tasks_from_file_type_opt(r"tests/resources/tasks_opt_case6.txt")
        expected_outcome = {
            0: ['1', '2'],
            1: ['3'],
            2: ['5'],
            3: ['6', '7'],
        }
        outcome = deduce_levels_of_tasks(tasks)
        self.assertEqual(expected_outcome, outcome)

    def test_extract_lines_from_file_type_opt_happy_case(self):
        ordered_tasks = extract_ordered_tasks_and_waits_from_file_type_opt(
            5,
            r"tests/resources/tasks_opt_happy_case.txt")

        expected_tasks = [
            OptimizedTask("1", "tm1srv01", "}bedrock.server.wait", {"pWaitSec": "1"}, [], True),
            Wait(),
            OptimizedTask("2", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "2"}, ["1"], True),
            Wait(),
            OptimizedTask("3", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "2"}, ["2"], True),
            Wait(),
            OptimizedTask("4", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "2"}, ["3"], True),
            Wait()]

        for (expected_task, ordered_task) in zip(expected_tasks, ordered_tasks):
            self.assertIsInstance(ordered_task, type(expected_task))

            if isinstance(expected_task, OptimizedTask):
                self.assertEqual(expected_task.id, ordered_task.id)
                self.assertEqual(expected_task.instance_name, ordered_task.instance_name)
                self.assertEqual(expected_task.process_name, ordered_task.process_name)
                self.assertEqual(expected_task.parameters, ordered_task.parameters)
                self.assertEqual(expected_task.predecessors, ordered_task.predecessors)
                self.assertEqual(expected_task.require_predecessor_success, ordered_task.require_predecessor_success)

    def test_extract_lines_from_file_type_opt_multi_task_per_id(self):
        ordered_tasks = extract_ordered_tasks_and_waits_from_file_type_opt(
            5,
            r"tests/resources/tasks_opt_multi_task_per_id.txt")

        expected_tasks = [
            OptimizedTask("1", "tm1srv01", "}bedrock.server.wait", {"pWaitSec": "1"}, [], True),
            Wait(),
            OptimizedTask("2", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "2"}, ["1"], True),
            OptimizedTask("2", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "3"}, ["1"], True),
            Wait(),
            OptimizedTask("3", "tm1srv02", "}bedrock.server.wait", {"pWaitSec": "4"}, ["2"], True),
            Wait()]

        for (expected_task, ordered_task) in zip(expected_tasks, ordered_tasks):
            self.assertIsInstance(ordered_task, type(expected_task))

            if isinstance(expected_task, OptimizedTask):
                self.assertEqual(expected_task.id, ordered_task.id)
                self.assertEqual(expected_task.instance_name, ordered_task.instance_name)
                self.assertEqual(expected_task.process_name, ordered_task.process_name)
                self.assertEqual(expected_task.parameters, ordered_task.parameters)
                self.assertEqual(expected_task.predecessors, ordered_task.predecessors)
                self.assertEqual(expected_task.require_predecessor_success, ordered_task.require_predecessor_success)
