import asyncio
import configparser
import datetime
import itertools
import logging
import os
import shlex
import sys
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from TM1py import TM1Service


def set_current_directory():
    abspath = os.path.abspath(__file__)
    directory = os.path.dirname(abspath)
    # set current directory
    os.chdir(directory)
    return directory


APP_NAME = "RushTI"
CURRENT_DIRECTORY = set_current_directory()
LOGFILE = os.path.join(CURRENT_DIRECTORY, APP_NAME + ".log")
CONFIG = os.path.join(CURRENT_DIRECTORY, "config.ini")


MSG_RUSHTI_STARTS = "{app_name} starts. Parameters: {parameters}."
MSG_RUSHTI_TOO_FEW_ARGUMENTS = "{app_name} needs to be executed with two arguments."
MSG_RUSHTI_ARGUMENT1_INVALID = "Argument 1 (path to file) invalid. File needs to exist."
MSG_RUSHTI_ARGUMENT2_INVALID = "Argument 2 (max workers) invalid. Argument needs to be an integer number."
MSG_RUSHTI_ARGUMENT3_INVALID = "Argument 3 (file type) invalid. Argument needs to be specified with the value 'n' or 's'."
MSG_PROCESS_EXECUTE = "Executing process: {process_name} with parameters: {parameters} on instance: {instance_name}"
MSG_PROCESS_SUCCESS = "Execution successful: Process {process} with parameters: {parameters} on instance: " \
                      "{instance}. Elapsed time: {time}"
MSG_PROCESS_FAIL_INSTANCE_NOT_AVAILABLE = "Process {process_name} not executed on {instance_name}. " \
                                          "{instance_name} not accessible."
MSG_PROCESS_FAIL_WITH_ERROR_FILE = "Execution failed. Process: {process} with parameters: {parameters} and status: " \
                                   "{status}, on instance: {instance}. Elapsed time : {time}. Error file: {error_file}"
MSG_PROCESS_FAIL_UNEXPECTED = "Execution failed. Process: {process} with parameters: {parameters}. " \
                              "Elapsed time: {time}. Error: {error}."
MSG_RUSHTI_ENDS = "{app_name} ends. {fails} fails out of {executions} executions. Elapsed time: {time}"


logging.basicConfig(
    filename=LOGFILE,
    format='%(asctime)s - ' + APP_NAME + ' - %(levelname)s - %(message)s',
    level=logging.INFO)


def setup_tm1_services(maximum_workers):
    """ Return Dictionary with TM1ServerName (as in config.ini) : Instantiated TM1Service
    
    :return: Dictionary server_names and TM1py.TM1Service instances pairs
    """
    if not os.path.isfile(CONFIG):
        raise ValueError("{config} does not exist.".format(config=CONFIG))
    tm1_services = dict()
    # parse .ini
    config = configparser.ConfigParser()
    config.read(CONFIG)
    # build tm1_services dictionary
    for tm1_server_name, params in config.items():
        # handle default values from configparser
        if tm1_server_name != config.default_section:
            try:
                tm1_services[tm1_server_name] = TM1Service(
                    **params,
                    session_context=APP_NAME,
                    connection_pool_size=maximum_workers)
            # Instance not running, Firewall or wrong connection parameters
            except Exception as e:
                logging.error("TM1 instance {} not accessible. Error: {}".format(tm1_server_name, str(e)))
    return tm1_services


def decrypt_password(encrypted_password):
    """ b64 decoding
    
    :param encrypted_password: encrypted password with b64
    :return: password in plain text
    """
    return b64decode(encrypted_password).decode("UTF-8")


def extract_info_from_line_type_s(line):
    """ Translate one line from txt file type 's' into arguments for execution

    :param: line: Arguments for execution. E.g. id="5" predecessors="2,3" instance="tm1srv01" process="Bedrock.Server.Wait" pWaitSec=5
    :return: attributes
    """
    attributes = {}
    temp = []
    for pair in shlex.split(line):
        attribute, value = pair.split("=")
        # if instance or process, needs to be case insensitive
        if attribute.lower() == 'process' or attribute.lower() == 'instance':
            attributes[attribute.lower()] = value.strip('"').strip()
        # Convert string attribute value into list
        elif attribute.lower() == 'predecessors':
            temp = value.strip('"').strip().split(',')
            if temp[0] == '':
                    attributes[attribute] = []
            else:
                    attributes[attribute] = temp
        # attributes (e.g. pWaitSec) are case sensitive in TM1 REST API !
        else:
            attributes[attribute] = value.strip('"').strip()

    return attributes


def extract_info_from_file_type_s(tasks_file_path):
    """ Read a file that respect type 's' specification and transform it into dictionary named tasks

    :param: tasks_file_path:
	:return: tasks
	"""
    tasks = {}
    task_attributes = {}
    with open(tasks_file_path) as input_file:
        lines = input_file.readlines()
        # Build tasks dictionnay
        for line in lines:
            task_attributes = extract_info_from_line_type_s(line)
            task_attributes["successors"] = []
            tasks[task_attributes["id"]] = task_attributes
        # Deduct the successors attribut and add it to the task_attributes
        for task in tasks.values():
            predecessors = task["predecessors"]
            if len(predecessors) != 0:
                for predecessor in predecessors:
                    successor = task["id"]
                    task_attributes = tasks[predecessor]
                    task_attributes["successors"].append(successor)
    return tasks


def deduce_levels_of_tasks(**tasks):
    """ Deduce the level of each task. Tasks at the same level have no relationship (successor / predecessor) between them

    :param: tasks:
	:return: levels
	"""
    levels = {}
    task_attributes = {}
    # level 0 contains all tasks without predecessors
    level = 0
    levels[level] = []
    for task in tasks.values():
        predecessors = task["predecessors"]
        if len(predecessors) == 0:
            levels[level].append(task["id"])
    # Handel other levels
    level = 0
    for task in tasks:
        level_tasks = levels[level]
        next_level_created = False
        for level_task in level_tasks:
            task_attributes = tasks[level_task]
            successors = task_attributes["successors"]
            # Create next level if necessary and add successors to this new level
            if len(successors) != 0:
                if not(next_level_created):
                    precedent_level = level
                    level += 1
                    levels[level] = []
                    next_level_created = True
                for successor in successors:
                    # test if task exists in current level
                    if not(successor in levels[level]):
                        levels[level].append(successor)
                    # Delet successor in precedent level
                    if successor in levels[precedent_level]:
                        levels[precedent_level].remove(successor)
    return levels


def rearrange_tasks_in_levels(maximum_workers, **tasks):
    """ Rearrange tasks across levels to optimize execution regarding the maximum workers. The constraint between tasks of same level (no relationship) must be conserved

    :param: tasks:
	:return: levels
	"""
    levels = deduce_levels_of_tasks(**tasks)
    levels_count = len(levels.items())
    for task_key in tasks.keys():
        index = 0
        while index < levels_count:
            level_key = index
            level = levels[level_key]
            if level_key + 1 < levels_count:
                next_level = levels[level_key + 1]
                if len(next_level) < int(maximum_workers):
                    for task in level:
                        successors = tasks[task]["successors"]
                        next_level_contains_successor = False
                        for successor in successors:
                            if next_level.count(successor) != 0:
                                next_level_contains_successor = True
                        if not(next_level_contains_successor):
                            # move task from level to next_level
                            levels[level_key].remove(task)
                            levels[level_key + 1].append(task)
            index += 1
    return levels


def get_lines(tasks_file_path, maximum_workers):
    """ Transform a file that respect type 's' specification into a scheduled and optimized list of tasks

    :param: tasks_file_path:
	:param: maximum_workers:
	:return: lines
    """
    lines = []
    if(tasks_file_type.lower() == "n"):
        with open(tasks_file_path) as file:
            lines = file.readlines()
    elif(tasks_file_type.lower() == "s"):
        tasks = {}
        levels = {}
        tasks = extract_info_from_file_type_s(tasks_file_path)
        levels = rearrange_tasks_in_levels(maximum_workers, **tasks)
        levels_count = len(levels.items())
        index_level = 0
        for level in levels.values():
            index_level += 1
            for task in level:
                task_attributes = tasks[task]
                line = ''
                for attribute_key, attribute_value in task_attributes.items():
                    if attribute_key.lower() != 'id' and attribute_key.lower() != 'predecessors' and attribute_key.lower() != 'successors':
                        line = line + attribute_key + '=' + '"' + attribute_value + '"' + ' '
                line = line + '\n'
                lines.append(line)
            if index_level < levels_count:
                line = 'wait\n'
                lines.append(line)
    return lines


def extract_info_from_line(line):
    """ Translate one line from txt file into arguments for execution: instance, process, parameters
    
    :param line: Arguments for execution. E.g. instance="tm1srv01" process="Bedrock.Server.Wait" pWaitSec=2
    :return: instance_name, process_name, parameters
    """
    parameters = {}
    for pair in shlex.split(line):
        param, value = pair.split("=")
        # if instance or process, needs to be case insensitive
        if param.lower() == 'process' or param.lower() == 'instance':
            parameters[param.lower()] = value.strip('"').strip()
        # parameters (e.g. pWaitSec) are case sensitive in TM1 REST API !
        else:
            parameters[param] = value.strip('"').strip()
    instance_name = parameters.pop("instance")
    process_name = parameters.pop("process")
    return instance_name, process_name, parameters


def execute_line(line, tm1_services):
    """ Execute one line from the txt file
    
    :param line: 
    :param tm1_services: 
    :return: 
    """
    if len(line.strip()) == 0:
        return True
    instance_name, process_name, parameters = extract_info_from_line(
        line)
    if instance_name not in tm1_services:
        msg = MSG_PROCESS_FAIL_INSTANCE_NOT_AVAILABLE.format(
            process_name=process_name,
            instance_name=instance_name)
        logging.error(msg)
        return False
    tm1 = tm1_services[instance_name]
    # Execute it
    msg = MSG_PROCESS_EXECUTE.format(
        process_name=process_name,
        parameters=parameters,
        instance_name=instance_name)
    logging.info(msg)
    start_time = datetime.datetime.now()
    try:
        success, status, error_log_file = tm1.processes.execute_with_return(
            process_name=process_name, **parameters)
        elapsed_time = datetime.datetime.now() - start_time
        if success:
            msg = MSG_PROCESS_SUCCESS
            msg = msg.format(
                process=process_name,
                parameters=parameters,
                instance=instance_name,
                time=elapsed_time)
            logging.info(msg)
            return True
        else:
            msg = MSG_PROCESS_FAIL_WITH_ERROR_FILE.format(
                process=process_name,
                parameters=parameters,
                status=status,
                instance=instance_name,
                time=elapsed_time,
                error_file=error_log_file)
            logging.error(msg)
            return False
    except Exception as e:
        elapsed_time = datetime.datetime.now() - start_time
        msg = MSG_PROCESS_FAIL_UNEXPECTED.format(
            process=process_name,
            parameters=parameters,
            error=str(e),
            time=elapsed_time)
        logging.error(msg)
        return False


async def work_through_tasks(tasks_file_path, maximum_workers, tasks_file_type, tm1_services):
    """ loop through file. Add all lines to the execution queue.
    
    :param tasks_file_path: 
    :param maximum_workers:
    :param tasks_file_type:
    :param tm1_services: 
    :return: 
    """
    lines = get_lines(tasks_file_path, maximum_workers)
    loop = asyncio.get_event_loop()
    # split lines into the blocks separated by 'wait' line
    line_sets = [list(y) for x, y in itertools.groupby(lines, lambda z: z.lower().strip() == 'wait') if not x]
    # True or False for every execution
    outcomes = []
    for line_set in line_sets:
        with ThreadPoolExecutor(int(maximum_workers)) as executor:
            futures = [loop.run_in_executor(executor, execute_line, line, tm1_services) for line in line_set]
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
    :return: tasks_file_path and maximum_workers
    """
    # too few arguments
    if len(args) < 4:
        msg = MSG_RUSHTI_TOO_FEW_ARGUMENTS.format(app_name=APP_NAME)
        logging.error(msg)
        sys.exit(msg)
    # txt file doesnt exist
    if not os.path.isfile(args[1]):
        msg = MSG_RUSHTI_ARGUMENT1_INVALID
        logging.error(msg)
        sys.exit(msg)
    # maximum_workers is not a number
    if not args[2].isdigit():
        msg = MSG_RUSHTI_ARGUMENT2_INVALID
        logging.error(msg)
        sys.exit(msg)
    # file type must be n or s
    if not (args[3].lower() == "n" or args[3].lower() == "s"):
        msg = MSG_RUSHTI_ARGUMENT3_INVALID
        logging.error(msg)
        sys.exit(msg)
    return args[1], args[2], args[3]


def exit_rushti(executions, successes, elapsed_time):
    """ Exit RushTI with exit code 0 or 1 depending on the TI execution outcomes

    :param executions: Number of executions
    :param successes: Number of executions that succeeded
    :param elapsed_time:
    :return:
    """
    fails = executions - successes
    message = MSG_RUSHTI_ENDS.format(
        app_name=APP_NAME,
        fails=fails,
        executions=executions,
        time=str(elapsed_time))
    if fails > 0:
        logging.error(message)
        sys.exit(message)
    else:
        logging.info(message)
        sys.exit(0)


# receives three arguments: 1) tasks_file_path, 2) maximum_workers, 3) tasks_file_type
if __name__ == "__main__":
    logging.info(MSG_RUSHTI_STARTS.format(
        app_name=APP_NAME,
        parameters=sys.argv))
    # start timer
    start = datetime.datetime.now()
    # read commandline arguments
    tasks_file_path, maximum_workers, tasks_file_type = translate_cmd_arguments(*sys.argv)
    # setup connections
    tm1_service_by_instance = setup_tm1_services(maximum_workers)
    # execution
    event_loop = asyncio.get_event_loop()
    try:
        results = event_loop.run_until_complete(
            work_through_tasks(tasks_file_path, maximum_workers, tasks_file_type, tm1_service_by_instance))
    finally:
        logout(tm1_service_by_instance)
        event_loop.close()
    # timing
    duration = datetime.datetime.now() - start
    exit_rushti(executions=len(results), successes=sum(results), elapsed_time=duration)
