import asyncio
import configparser
import csv
import functools
import itertools
import logging
import os
import shlex
import sys
import uuid
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from logging.config import fileConfig
from typing import List, Union, Dict

try:
    import chardet
except ImportError:
    pass

from TM1py import TM1Service

from utils import set_current_directory, Task, OptimizedTask, ExecutionMode, Wait

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
    "'{instance_name}' not defined in provided config file. Check for typos and miscapitalization.")
MSG_PROCESS_FAIL_WITH_ERROR_FILE = (
    "Execution failed. Process: '{process}' with parameters: {parameters} with {retries} retries and status: "
    "{status}, on instance: '{instance}'. Elapsed time : {time}. Error file: {error_file}")
MSG_PROCESS_FAIL_UNEXPECTED = (
    "Execution failed. Process: '{process}' with parameters: {parameters}. "
    "Elapsed time: {time}. Error: {error}.")
MSG_RUSHTI_ENDS = ("{app_name} ends. {fails} fails out of {executions} executions. "
                   "Elapsed time: {time}. Ran with parameters: {parameters}")
MSG_RUSHTI_ABORTED = "{app_name} aborted with error"
MSG_PROCESS_ABORTED_FAILED_PREDECESSOR = (
    "Execution aborted. Process: '{process}' with parameters: {parameters} is not run "
    "due to failed predecessor {predecessor}, on instance: '{instance}'")
MSG_PROCESS_ABORTED_UNCOMPLETE_PREDECESSOR = (
    "Execution aborted. Process: '{process}' with parameters: {parameters} is not run "
    "due to uncompleted predecessor {predecessor}, on instance: '{instance}'")
MSG_PROCESS_NOT_EXISTS = (
    "Task validation failed. Process: '{process}' does not exist on instance: '{instance}'")
MSG_PROCESS_PARAMS_INCORRECT = (
    "Task validation failed. Process: '{process}' does not have: {parameters}, "
    "on instance: '{instance}'")

# used to wrap blackslashes before using
UNIQUE_STRING = uuid.uuid4().hex[:8].upper()

TRUE_VALUES = ["1", "y", "yes", "true", "t"]

if not os.path.isfile(LOGGING_CONFIG):
    raise ValueError("{config} does not exist".format(config=LOGGING_CONFIG))
fileConfig(LOGGING_CONFIG)
logger = logging.getLogger()

# store execution results per line id to control predecessor dependant lines
TASK_EXECUTION_RESULTS = dict()


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
    config.read(CONFIG, encoding='utf-8')
    # build tm1_services dictionary
    for tm1_server_name, params in config.items():
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
    tasks = get_ordered_tasks_and_waits(file_path=tasks_file_path, max_workers=max_workers,
                                        tasks_file_type=execution_mode)
    for task in tasks:
        if isinstance(task, Wait):
            continue

        tm1_instances_in_tasks.add(task.instance_name)
    return tm1_instances_in_tasks


def decrypt_password(encrypted_password: str) -> str:
    """ b64 decoding

    :param encrypted_password: encrypted password with b64
    :return: password in plain text
    """
    return b64decode(encrypted_password).decode("UTF-8")


def extract_task_or_wait_from_line(line: str) -> Union[Task, Wait]:
    """ Translate one line from txt file into arguments for execution: instance, process, parameters
    :param line: Arguments for execution. E.g. instance="tm1srv01" process="Bedrock.Server.Wait" pWaitSec=2
    :return: instance_name, process_name, parameters
    """
    if line.strip().lower() == 'wait':
        return Wait()

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


def extract_task_from_line_type_opt(line: str) -> OptimizedTask:
    """ Translate one line from txt file type 'opt' into arguments for execution
    :param: line: Arguments for execution. E.g. id="5" predecessors="2,3" require_predecessor_success="1" instance="tm1srv01"
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

        elif argument.lower() == "require_predecessor_success":
            line_arguments["require_predecessor_success"] = value.strip('"').strip().lower() in TRUE_VALUES

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
        require_predecessor_success=line_arguments.pop("require_predecessor_success", False),
        parameters=line_arguments)


def extract_ordered_tasks_and_waits_from_file_type_norm(file_path: str):
    with open(file_path, encoding='utf-8') as file:
        return [extract_task_or_wait_from_line(line) for line in file.readlines()]


def extract_ordered_tasks_and_waits_from_file_type_opt(max_workers: int, file_path: str) -> List[Task]:
    ordered_tasks_and_waits = list()
    tasks = extract_tasks_from_file_type_opt(file_path)

    # mapping of level (int) against list of tasks
    tasks_by_level = deduce_levels_of_tasks(tasks)
    # balance levels
    tasks_by_level = balance_tasks_among_levels(max_workers, tasks, tasks_by_level)
    for level in tasks_by_level.values():
        for task_id in level:
            for task in tasks[task_id]:
                ordered_tasks_and_waits.append(task)

        ordered_tasks_and_waits.append(Wait())
    return ordered_tasks_and_waits


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
            task = extract_task_from_line_type_opt(line)
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


def pre_process_file(file_path: str):
    """ Preprocess file for Python to change encoding from 'utf-8-sig' to 'utf-8'

    Background: Under certain circumstances TM1 / Turbo Integrator generates files with utf-8-sig

    :param file_path:
    :return:
    """
    with open(file_path, 'rb') as file:
        raw = file.read(32)  # at most 32 bytes are returned
        encoding = chardet.detect(raw)['encoding']

    if encoding.upper() == 'UTF-8-SIG':
        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            text = file.read()
        with open(file_path, mode='w', encoding='utf-8') as file:
            file.write(text)


def get_ordered_tasks_and_waits(file_path: str, max_workers: int, tasks_file_type: ExecutionMode) -> List[Task]:
    """ Extract tasks from file
    if necessary transform a file that respects type 'opt' specification into a scheduled and optimized list of tasks
    :param file_path:
    :param max_workers:
    :param tasks_file_type:
    :return:
    """
    try:
        import chardet
        pre_process_file(file_path)

    except ImportError:
        logging.info(f"Function '{pre_process_file.__name__}' skipped. Optional dependency 'chardet' not installed")

    if tasks_file_type == ExecutionMode.NORM:
        return extract_ordered_tasks_and_waits_from_file_type_norm(file_path)
    else:
        return extract_ordered_tasks_and_waits_from_file_type_opt(max_workers, file_path)


def execute_process_with_retries(tm1: TM1Service, task: Task, retries: int):
    attempt = 0
    while attempt <= retries:
        try:
            # process runs and returns either success = True or False
            success, status, error_log_file = tm1.processes.execute_with_return(
                process_name=task.process_name,
                **task.parameters)
        except Exception as e:
            # process could not be executed (e.g. doesn't exist)
            # If last attempt, then raise exception
            if attempt == retries:
                raise e
        else:
            if success:
                break
        finally:
            attempt += 1

    return success, status, error_log_file, attempt


def update_task_execution_results(func):
    @functools.wraps(func)
    def wrapper(task: Task, *args, **kwargs):
        task_success = False
        try:
            task_success = func(task, *args, **kwargs)

        finally:
            # two optimized tasks can have the same id !
            previous_task_success = TASK_EXECUTION_RESULTS.get(task.id, True)
            TASK_EXECUTION_RESULTS[task.id] = previous_task_success and task_success

            return task_success

    return wrapper


@update_task_execution_results
def execute_task(task: Task, retries: int, tm1_services: Dict[str, TM1Service]) -> bool:
    """ Execute one line from the txt file
    :param task:
    :param retries:
    :param tm1_services:
    :return:
    """

    # check predecessors success
    if isinstance(task, OptimizedTask) and task.require_predecessor_success:
        predecessors_ok = verify_predecessors_ok(task)
        if not predecessors_ok:
            return False

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
    start_time = datetime.now()

    try:
        success, status, error_log_file, attempts = execute_process_with_retries(
            tm1=tm1, task=task, retries=retries)
        elapsed_time = datetime.now() - start_time

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
        elapsed_time = datetime.now() - start_time
        msg = MSG_PROCESS_FAIL_UNEXPECTED.format(
            process=task.process_name, parameters=task.parameters, error=str(e), time=elapsed_time)
        logger.error(msg)
        return False


def verify_predecessors_ok(task: OptimizedTask) -> bool:
    for predecessor_id in task.predecessors:

        if predecessor_id not in TASK_EXECUTION_RESULTS:
            msg = MSG_PROCESS_ABORTED_UNCOMPLETE_PREDECESSOR.format(
                instance=task.instance_name,
                process=task.process_name,
                parameters=task.parameters,
                predecessor=predecessor_id)
            logger.error(msg)
            return False

        if not TASK_EXECUTION_RESULTS[predecessor_id]:
            msg = MSG_PROCESS_ABORTED_FAILED_PREDECESSOR.format(
                instance=task.instance_name,
                process=task.process_name,
                parameters=task.parameters,
                predecessor=predecessor_id)
            logger.error(msg)
            return False

    return True


def validate_tasks(tasks: List[Task], tm1_services: Dict[str, TM1Service]) -> bool:
    validated_tasks = []
    validation_ok = True

    tasks = [task for task in tasks if isinstance(task, Task)]  # --> ignore Wait(s)
    for task in tasks:
        current_task = {
            'instance': task.instance_name,
            'process': task.process_name,
            'parameters': task.parameters.keys()
        }

        tm1 = tm1_services[task.instance_name]

        # avoid repeated validations
        if current_task in validated_tasks:
            continue

        # check for process existence
        if not tm1.processes.exists(task.process_name):
            msg = MSG_PROCESS_NOT_EXISTS.format(
                process=task.process_name,
                instance=task.instance_name
            )
            logger.error(msg)
            validated_tasks.append(current_task)
            validation_ok = False
            continue

        # check for parameters
        task_params = task.parameters.keys()
        if task_params:
            process_params = [param['Name'] for param in tm1.processes.get(task.process_name).parameters]

            # check for missing parameter names
            missing_params = [param for param in task_params if param not in process_params]
            if len(missing_params) > 0:
                msg = MSG_PROCESS_PARAMS_INCORRECT.format(
                    process=task.process_name,
                    parameters=missing_params,
                    instance=task.instance_name
                )
                logger.error(msg)
                validation_ok = False

        validated_tasks.append(current_task)

    return validation_ok


async def work_through_tasks(max_workers: int, retries: int, tm1_services: dict):
    """ loop through file. Add all lines to the execution queue.
    :param max_workers:
    :param retries:
    :param tm1_services:
    :return:
    """

    # split lines into the blocks separated by 'wait' line
    task_sets = [
        list(y)
        for x, y in itertools.groupby(tasks, lambda z: isinstance(z, Wait))
        if not x]

    # True or False for every execution
    outcomes = []

    loop = asyncio.get_event_loop()

    for task_set in task_sets:
        with ThreadPoolExecutor(int(max_workers)) as executor:
            futures = [
                loop.run_in_executor(executor, execute_task, task, retries, tm1_services)
                for task
                in task_set]

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
    :return: tasks_file_path, maximum_workers, execution_mode, retries, result_file
    """
    # too few arguments
    if len(args) < 3 or len(args) > 6:
        msg = MSG_RUSHTI_WRONG_NUMBER_OF_ARGUMENTS.format(app_name=APP_NAME)
        logger.error(msg)
        sys.exit(msg)

    # default values
    mode = ExecutionMode.NORM
    retries = 0
    result_file = "rushti.csv"

    # txt file doesnt exist
    tasks_file = args[1]
    if not os.path.isfile(tasks_file):
        msg = MSG_RUSHTI_ARGUMENT1_INVALID
        logger.error(msg)
        sys.exit(msg)

    # maximum_workers is not a number
    max_workers = args[2]
    if not max_workers.isdigit():
        msg = MSG_RUSHTI_ARGUMENT2_INVALID
        logger.error(msg)
        sys.exit(msg)
    else:
        max_workers = int(max_workers)

    if len(args) == 4:
        try:
            mode = ExecutionMode(args[3])
        except ValueError:
            msg = MSG_RUSHTI_ARGUMENT3_INVALID
            logger.error(msg)
            sys.exit(msg)

    if len(args) == 5:
        retries = args[4]
        if not retries.isdigit():
            msg = MSG_RUSHTI_ARGUMENT4_INVALID
            logger.error(msg)
            sys.exit(msg)
        else:
            retries = int(retries)

    if len(args) == 6:
        result_file = args[5]

    return tasks_file, max_workers, mode, retries, result_file


def create_results_file(
        result_file: str,
        overall_success: bool,
        executions: int,
        fails: int,
        start_time: datetime,
        end_time: datetime,
        elapsed_time: timedelta):
    header = (
        "PID", "Process Runs", "Process Fails",
        "Start", "End", "Runtime", "Overall Success")
    record = (
        os.getpid(), executions, fails,
        start_time, end_time, elapsed_time, overall_success)

    with open(result_file, "w", encoding="utf-8") as file:
        cw = csv.writer(file, delimiter='|')
        cw.writerows([header, record])


def exit_rushti(
        overall_success: bool,
        executions: int,
        successes: int,
        start_time: datetime,
        end_time: datetime,
        elapsed_time: timedelta):
    """ Exit RushTI with exit code 0 or 1 depending on the TI execution outcomes
    :param overall_success: Exception raised during executions
    :param executions: Number of executions
    :param successes: Number of executions that succeeded
    :param start_time:
    :param end_time:
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

    create_results_file(
        result_file,
        overall_success,
        executions,
        fails,
        start_time,
        end_time,
        elapsed_time)

    if fails > 0:
        logger.error(message)
        sys.exit(message)

    logger.info(message)
    sys.exit(0)


# receives three arguments: 1) tasks_file_path, 2) maximum_workers, 3) execution_mode, 4) retries
if __name__ == "__main__":
    logger.info(MSG_RUSHTI_STARTS.format(app_name=APP_NAME, parameters=sys.argv))
    # start timer
    start = datetime.now()

    # read commandline arguments
    (tasks_file_path, maximum_workers, execution_mode,
     process_execution_retries, result_file) = translate_cmd_arguments(*sys.argv)

    # setup connections
    tm1_service_by_instance = setup_tm1_services(maximum_workers, tasks_file_path, execution_mode)

    # setup results variable (guarantee it's not empty in case of error)
    results = list()

    # spawn event loop for parallelization
    event_loop = None

    try:
        # determine and validate tasks
        tasks = get_ordered_tasks_and_waits(tasks_file_path, maximum_workers, execution_mode)
        if not validate_tasks(tasks, tm1_service_by_instance):
            raise ValueError("Invalid tasks provided")

        # execution
        event_loop = asyncio.new_event_loop()
        results = event_loop.run_until_complete(
            work_through_tasks(
                maximum_workers,
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
        if event_loop:
            event_loop.close()

    # timing
    end = datetime.now()
    duration = end - start
    exit_rushti(
        overall_success=success,
        executions=len(results),
        successes=sum(results),
        start_time=start,
        end_time=end,
        elapsed_time=duration)
