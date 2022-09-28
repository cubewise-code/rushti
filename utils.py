import os
import sys
from enum import Enum
from typing import List, Dict


def set_current_directory():
    # determine if application is a script file or frozen exe
    if getattr(sys, 'frozen', False):
        application_path = os.path.abspath(sys.executable)
    elif __file__:
        application_path = os.path.abspath(__file__)

    directory = os.path.dirname(application_path)
    # set current directory
    os.chdir(directory)
    return directory


class Wait:
    def __init__(self):
        pass

    # useful for testing
    def __eq__(self, other):
        if isinstance(other, Wait):
            return True

        return False


class Task:
    id = 1

    def __init__(self, instance_name, process_name, parameters):
        self.id = Task.id
        self.instance_name = instance_name
        self.process_name = process_name
        self.parameters = parameters

        Task.id = Task.id + 1

    def translate_to_line(self):
        return 'instance="{instance}" process="{process}" {parameters}\n'.format(
            instance=self.instance_name,
            process=self.process_name,
            parameters=' '.join('{}="{}"'.format(parameter, value) for parameter, value in self.parameters.items()))


class OptimizedTask(Task):
    def __init__(self, task_id: str, instance_name: str, process_name: str, parameters: Dict, predecessors: List,
                 require_predecessor_success: bool):
        super().__init__(instance_name, process_name, parameters)
        self.id = task_id
        self.predecessors = predecessors
        self.require_predecessor_success = require_predecessor_success
        self.successors = list()

    @property
    def has_predecessors(self):
        return len(self.predecessors) > 0

    @property
    def has_successors(self):
        return len(self.successors) > 0


class ExecutionMode(Enum):
    NORM = 1
    OPT = 2

    @classmethod
    def _missing_(cls, value):
        for member in cls:
            if member.name.lower() == value.lower():
                return member
        # default
        return cls.NORM
