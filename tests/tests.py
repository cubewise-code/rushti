import unittest

from RushTI import deduce_levels_of_tasks, extract_tasks_from_file_type_opt


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
