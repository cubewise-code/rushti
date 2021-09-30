import asyncio
import configparser
import datetime
import itertools
import logging
import os
import shlex
import sys
import uuid
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from logging.config import fileConfig

from TM1py import TM1Service

from utils import set_current_directory, Task, OptimizedTask, ExecutionMode

APP_NAME = "RushTI"
CURRENT_DIRECTORY = set_current_directory()
LOGFILE = os.path.join(CURRENT_DIRECTORY, APP_NAME + ".log")
CONFIG = os.path.join(CURRENT_DIRECTORY, "config.ini")
LOGGING_CONFIG = os.path.join(CURRENT_DIRECTORY, 'logging_config.ini')

MSG_RUSHTI_STARTS = "{app_name} starts. Parameters: {parameters}."
MSG_RUSHTI_WRONG_NUMBER_OF_ARGUMENTS = "{app_name} needs to be executed with two to four arguments."
MSG_RUSHTI_ARGUMENT1_INVALID = "Argument 1 (path to tasks file) invalid. File needs to exist."
MSG_RUSHTI_ARGUMENT2_INVALID = "Argument 2 (maximum workers) invalid. Argument must be an integer number."
MSG_RUSHTI_ARGUMENT3_INVALID = "Argument 3 (tasks file type) invalid. Argument can be 'opt' or 'norm'."
MSG_RUSHTI_ARGUMENT4_INVALID = "Argument 4 (retries) invalid. Argument must be an integer number."
MSG_PROCESS_EXECUTE = "Executing process: '{process_name}' with parameters: {parameters} on instance: '{instance_name}'"
MSG_PROCESS_SUCCESS = (
    "Execution successful: Process '{process}' with parameters: {parameters} with {retries} retries on instance: "
    "{instance}. Elapsed time: {time}")
MSG_PROCESS_FAIL_INSTANCE_NOT_IN_CONFIG_FILE = (
    "Process '{process_name}' not executed on '{instance_name}'. "
    "'{instance_name}' not defined in provided config file. Check for typos and capitalization.")
MSG_PROCESS_FAIL_WITH_ERROR_FILE = (
    "Execution failed. Process: '{process}' with parameters: {parameters} with {retries} retries and status: "
    "{status}, on instance: '{instance}'. Elapsed time : {time}. Error file: {error_file}")
MSG_PROCESS_FAIL_UNEXPECTED = (
    "Execution failed. Process: '{process}' with parameters: {parameters}. "
    "Elapsed time: {time}. Error: {error}.")
MSG_RUSHTI_ENDS = ("{app_name} ends. {fails} fails out of {executions} executions. "
                   "Elapsed time: {time}. Ran with parameters: {parameters}")
MSG_RUSHTI_ABORTED = "{app_name} aborted with error"

# used to wrap blackslashes before using
UNIQUE_STRING = uuid.uuid4().hex[:8].upper()

if not os.path.isfile(LOGGING_CONFIG):
    raise ValueError("{config} does not exist".format(config=LOGGING_CONFIG))
fileConfig(LOGGING_CONFIG)
logger = logging.getLogger()


def setup_tm1_services(max_workers: int, tasks_file_path: str, execution_mode: ExecutionMode) -> dict:
    """ Return Dictionary with TM1ServerName (as in config.ini) : Instantiated TM1Service
    
    :return: Dictionary server_names and TM1py.TM1Service instances pairs
    """
    if not os.path.isfile(CONFIG):
        raise ValueError("{config} does not exist".format(config=CONFIG))

    tm1_instances_in_tasks = get_instances_from_tasks_file(execution_mode, max_workers, tasks_file_path)

    tm1_services = dict()
    # parse .ini
    config = configparser.ConfigParser()
    config.read(CONFIG)
    # build tm1_services dictionary
    for tm1_server_name, params in config.items():
        logger.info(tm1_server_name)
        if tm1_server_name not in tm1_instances_in_tasks:
            continue

        # handle default values from configparser
        if tm1_server_name != config.default_section:
            try:
                tm1_services[tm1_server_name] = TM1Service(
                    **params,
                    session_context=APP_NAME,
                    connection_pool_size=max_workers)
            # Instance not running, Firewall or wrong connection parameters
            except Exception as e:
                logger.error(
                    "TM1 instance {} not accessible. Error: {}".format(
                        tm1_server_name, str(e)))
    return tm1_services


def get_instances_from_tasks_file(execution_mode, max_workers, tasks_file_path):
    tm1_instances_in_tasks = set()
    lines = get_task_lines(file_path=tasks_file_path, max_workers=max_workers, tasks_file_type=execution_mode)
    for line in lines:
        if line.lower().strip() == 'wait':
            continue
        task = extract_task_from_line(line)
        tm1_instances_in_tasks.add(task.instance_name)
    return tm1_instances_in_tasks


def decrypt_password(encrypted_password: str) -> str:
    """ b64 decoding
    
    :param encrypted_password: encrypted password with b64
    :return: password in plain text
    """
    return b64decode(encrypted_password).decode("UTF-8")


def extract_task_from_line(line: str) -> Task:
    """ Translate one line from txt file into arguments for execution: instance, process, parameters
    :param line: Arguments for execution. E.g. instance="tm1srv01" process="Bedrock.Server.Wait" pWaitSec=2
    :return: instance_name, process_name, parameters
    """
    line_arguments = dict()
    line = line.replace("\\", UNIQUE_STRING)
    for pair in shlex.split(line):
        param, value = pair.split("=")
        param = param.replace(UNIQUE_STRING, "\\")
        value = value.replace(UNIQUE_STRING, "\\")

        # if instance or process, needs to be case insensitive
        if param.lower() == "process" or param.lower() == "instance":
            line_arguments[param.lower()] = value.strip('"').strip()
        # parameters (e.g. pWaitSec) are case sensitive in TM1 REST API !
        else:
            line_arguments[param] = value.strip('"').strip()
    return Task(
        instance_name=line_arguments.pop("instance"),
        process_name=line_arguments.pop("process"),
        parameters=line_arguments)


def extract_tasks_from_line_type_opt(line: str) -> OptimizedTask:
    """ Translate one line from txt file type 'opt' into arguments for execution
    :param: line: Arguments for execution. E.g. id="5" predecessors="2,3" instance="tm1srv01"
    process="Bedrock.Server.Wait" pWaitSec=5
    :return: attributes
    """
    line_arguments = dict()
    line = line.replace("\\", UNIQUE_STRING)
    for pair in shlex.split(line):
        argument, value = pair.split("=")
        argument = argument.replace(UNIQUE_STRING, "\\")
        value = value.replace(UNIQUE_STRING, "\\")

        # if instance or process, needs to be case insensitive
        if argument.lower() == "process" or argument.lower() == "instance" or argument.lower() == "id":
            line_arguments[argument.lower()] = value.strip('"').strip()
        # Convert string attribute value into list
        elif argument.lower() == "predecessors":
            predecessors = value.strip('"').strip().split(",")
            # "", "0" and 0 is understood as 'no predecessor'
            if predecessors[0] in ["", "0", 0]:
                line_arguments[argument] = []
            else:
                line_arguments[argument] = predecessors
        # parameters (e.g. pWaitSec) are case sensitive in TM1 REST API !
        else:
            line_arguments[argument] = value.strip('"').strip()

    return OptimizedTask(
        task_id=line_arguments.pop("id"),
        instance_name=line_arguments.pop("instance"),
        process_name=line_arguments.pop("process"),
        predecessors=line_arguments.pop("predecessors"),
        parameters=line_arguments)


def extract_lines_from_file_type_opt(max_workers: int, file_path: str) -> list:
    lines = list()
    tasks = extract_tasks_from_file_type_opt(file_path)

    # mapping of level (int) against list of tasks
    tasks_by_level = deduce_levels_of_tasks(tasks)
    # balance levels
    tasks_by_level = balance_tasks_among_levels(max_workers, tasks, tasks_by_level)
    for level in tasks_by_level.values():
        for task_id in level:
            for task in tasks[task_id]:
                line = task.translate_to_line()
                lines.append(line)
        lines.append("wait\n")
    return lines


def extract_tasks_from_file_type_opt(file_path: str) -> dict:
    """
    :param file_path:
    :return: tasks
    """
    # Mapping of id against task
    tasks = dict()
    with open(file_path, encoding='utf-8') as input_file:
        lines = input_file.readlines()
        # Build tasks dictionary
        for line in lines:
            # skip empty lines
            if not line.strip():
                continue
            task = extract_tasks_from_line_type_opt(line)
            if task.id not in tasks:
                tasks[task.id] = [task]
            else:
                tasks[task.id].append(task)

    # Populate the successors attribute
    for task_id in tasks:
        for task in tasks[task_id]:
            predecessors = task.predecessors
            for predecessor_id in predecessors:
                for pre_task in tasks[predecessor_id]:
                    pre_task.successors.append(task.id)
    return tasks


def deduce_levels_of_tasks(tasks: dict) -> dict:
    """ Deduce the level of each task.
    Tasks at the same level have no relationship (successor / predecessor) between them
    :param tasks: mapping of id against Task
    :return: levels
    """

    levels = dict()
    # level 0 contains all tasks without predecessors
    level = 0
    levels[level] = list()
    for task_id in tasks:
        for task in tasks[task_id]:
            if not task.has_predecessors:
                # avoid duplicates
                if task.id not in levels[level]:
                    levels[level].append(task.id)

    # Handle other levels. Iterative approach.
    for _ in tasks:
        task_ids_in_level = levels[level]
        next_level_created = False
        for task_id in task_ids_in_level:
            for task in tasks[task_id]:
                # Create next level if necessary and add successors to this new level
                if task.has_successors:
                    if not next_level_created:
                        precedent_level = level
                        level += 1
                        levels[level] = list()
                        next_level_created = True

                    for successor in task.successors:
                        # test if task exists in current level
                        if not (successor in levels[level]):
                            # avoid duplicates
                            if successor not in levels[level]:
                                levels[level].append(successor)

                        # Delete successor in precedent level
                        if successor in levels[precedent_level]:
                            levels[precedent_level].remove(successor)

    return levels


def balance_tasks_among_levels(max_workers: int, tasks: dict, levels: dict):
    """Rearrange tasks across levels to optimize execution regarding the maximum workers.
    The constraint between tasks of same level (no relationship) must be conserved
    :param tasks:
    :param max_workers:
    :param levels:
    :return:
    """

    levels_count = len(levels)
    for _ in levels:
        for level_key in range(levels_count - 1):
            level = levels[level_key]
            next_level = levels[level_key + 1]
            if len(level) >= max_workers >= len(next_level):
                for task_id in level:
                    for task in tasks[task_id]:
                        successors = task.successors
                        # if next level contains successor don't move this task
                        next_level_contains_successor = False
                        for successor in successors:
                            if successor in next_level:
                                next_level_contains_successor = True

                        if not next_level_contains_successor:
                            # move task from level to next_level
                            if task_id in levels[level_key]:
                                levels[level_key].remove(task_id)
                            if task_id not in levels[level_key + 1]:
                                levels[level_key + 1].append(task_id)
    return levels


def get_task_lines(file_path: str, max_workers: int, tasks_file_type: ExecutionMode) -> list:
    """ Extract tasks from file
    if necessary transform a file that respects type 'opt' specification into a scheduled and optimized list of tasks
    :param file_path:
    :param max_workers:
    :param tasks_file_type:
    :return:
    """
    if tasks_file_type == ExecutionMode.NORM:
        with open(file_path, encoding='utf-8') as file:
            return file.readlines()
    else:
        return extract_lines_from_file_type_opt(max_workers, file_path)


def execute_process_with_retries(tm1: TM1Service, task: Task, retries: int):
    attempt = 0
    while True:
        try:
            success, status, error_log_file = tm1.processes.execute_with_return(
                process_name=task.process_name,
                **task.parameters)
            if success:
                return success, status, error_log_file, attempt
            if attempt == retries:
                return success, status, error_log_file, attempt
        except:
            continue
        finally:
            attempt += 1


def execute_line(line, retries, tm1_services):
    """ Execute one line from the txt file
    :param line:
    :param retries:
    :param tm1_services: 
    :return: 
    """
    if len(line.strip()) == 0:
        return True
    task = extract_task_from_line(line)
    if task.instance_name not in tm1_services:
        msg = MSG_PROCESS_FAIL_INSTANCE_NOT_IN_CONFIG_FILE.format(
            process_name=task.process_name, instance_name=task.instance_name)
        logger.error(msg)
        return False
    tm1 = tm1_services[task.instance_name]
    # Execute it
    msg = MSG_PROCESS_EXECUTE.format(
        process_name=task.process_name, parameters=task.parameters, instance_name=task.instance_name)
    logger.info(msg)
    start_time = datetime.datetime.now()
    try:
        success, status, error_log_file, attempts = execute_process_with_retries(
            tm1=tm1, task=task, retries=retries)
        elapsed_time = datetime.datetime.now() - start_time
        if success:
            msg = MSG_PROCESS_SUCCESS
            msg = msg.format(
                process=task.process_name,
                parameters=task.parameters,
                instance=task.instance_name,
                retries=attempts,
                time=elapsed_time)
            logger.info(msg)
            return True
        else:
            msg = MSG_PROCESS_FAIL_WITH_ERROR_FILE.format(
                process=task.process_name,
                parameters=task.parameters,
                status=status,
                instance=task.instance_name,
                retries=attempts,
                time=elapsed_time,
                error_file=error_log_file)
            logger.error(msg)
            return False
    except Exception as e:
        elapsed_time = datetime.datetime.now() - start_time
        msg = MSG_PROCESS_FAIL_UNEXPECTED.format(
            process=task.process_name, parameters=task.parameters, error=str(e), time=elapsed_time)
        logger.error(msg)
        return False


async def work_through_tasks(file_path: str, max_workers: int, mode: ExecutionMode, retries: int, tm1_services: dict):
    """ loop through file. Add all lines to the execution queue.
    :param file_path:
    :param max_workers:
    :param mode:
    :param retries:
    :param tm1_services: 
    :return: 
    """
    lines = get_task_lines(file_path, max_workers, mode)
    loop = asyncio.get_event_loop()
    # split lines into the blocks separated by 'wait' line
    line_sets = [
        list(y)
        for x, y in itertools.groupby(lines, lambda z: z.lower().strip() == "wait")
        if not x]

    # True or False for every execution
    outcomes = []
    for line_set in line_sets:
        with ThreadPoolExecutor(int(max_workers)) as executor:
            futures = [
                loop.run_in_executor(executor, execute_line, line, retries, tm1_services)
                for line
                in line_set]
            for future in futures:
                outcomes.append(await future)
    return outcomes


def logout(tm1_services):
    """ logout from all instances
     
    :param tm1_services: 
    :return: 
    """
    for tm1 in tm1_services.values():
        tm1.logout()


def translate_cmd_arguments(*args):
    """ Translation and Validity-checks for command line arguments.
    
    :param args: 
    :return: tasks_file_path, maximum_workers, execution_mode
    """
    # too few arguments
    if len(args) < 3 or len(args) > 5:
        msg = MSG_RUSHTI_WRONG_NUMBER_OF_ARGUMENTS.format(app_name=APP_NAME)
        logger.error(msg)
        sys.exit(msg)

    # txt file doesnt exist
    if not os.path.isfile(args[1]):
        msg = MSG_RUSHTI_ARGUMENT1_INVALID
        logger.error(msg)
        sys.exit(msg)

    # maximum_workers is not a number
    if not args[2].isdigit():
        msg = MSG_RUSHTI_ARGUMENT2_INVALID
        logger.error(msg)
        sys.exit(msg)

    # default values
    mode = ExecutionMode.NORM
    retries = 0

    if len(args) == 3:
        return args[1], int(args[2]), mode, retries

    try:
        mode = ExecutionMode(args[3])
    except ValueError:
        msg = MSG_RUSHTI_ARGUMENT3_INVALID
        logger.error(msg)
        sys.exit(msg)

    if len(args) == 4:
        return args[1], int(args[2]), mode, retries

    if not args[4].isdigit():
        msg = MSG_RUSHTI_ARGUMENT4_INVALID
        logger.error(msg)
        sys.exit(msg)

    return args[1], int(args[2]), mode, int(args[4])


def exit_rushti(overall_success, executions, successes, elapsed_time):
    """ Exit RushTI with exit code 0 or 1 depending on the TI execution outcomes
    :param overall_success: Exception raised during executions
    :param executions: Number of executions
    :param successes: Number of executions that succeeded
    :param elapsed_time:
    :return:
    """
    if not overall_success:
        message = MSG_RUSHTI_ABORTED.format(app_name=APP_NAME)
        logger.error(message)
        sys.exit(message)

    fails = executions - successes
    message = MSG_RUSHTI_ENDS.format(
        app_name=APP_NAME, fails=fails, executions=executions, time=str(elapsed_time), parameters=sys.argv
    )
    if fails > 0:
        logger.error(message)
        sys.exit(message)

    logger.info(message)
    sys.exit(0)


# receives three arguments: 1) tasks_file_path, 2) maximum_workers, 3) execution_mode, 4) retries
if __name__ == "__main__":
    logger.info(MSG_RUSHTI_STARTS.format(app_name=APP_NAME, parameters=sys.argv))
    # start timer
    start = datetime.datetime.now()
    # read commandline arguments
    tasks_file_path, maximum_workers, execution_mode, process_execution_retries = translate_cmd_arguments(*sys.argv)
    # setup connections
    tm1_service_by_instance = setup_tm1_services(maximum_workers, tasks_file_path, execution_mode)
    # setup results variable (guarantee it's not empty in case of error)
    results = list()
    # execution
    event_loop = asyncio.get_event_loop()
    try:
        results = event_loop.run_until_complete(
            work_through_tasks(
                tasks_file_path,
                maximum_workers,
                execution_mode,
                process_execution_retries,
                tm1_service_by_instance
            )
        )
        success = True

    except:
        logging.exception("Fatal Error")
        success = False

    finally:
        logout(tm1_service_by_instance)
        event_loop.close()

    # timing
    duration = datetime.datetime.now() - start
    exit_rushti(overall_success=success, executions=len(results), successes=sum(results), elapsed_time=duration)
