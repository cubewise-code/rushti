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


def setup_tm1_services():
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
                params["password"] = decrypt_password(params["password"])
                tm1_services[tm1_server_name] = TM1Service(**params, session_context=APP_NAME)
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
    instance_name, process_name, parameters = extract_info_from_line(line)
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
        success, status, error_log_file = tm1.processes.execute_with_return(process_name=process_name, **parameters)
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


async def work_through_tasks(path, max_workers, tm1_services):
    """ loop through file. Add all lines to the execution queue.
    
    :param path: 
    :param max_workers: 
    :param tm1_services: 
    :return: 
    """
    with open(path) as file:
        lines = file.readlines()
    loop = asyncio.get_event_loop()

    # split lines into the blocks separated by 'wait' line
    line_sets = [list(y) for x, y in itertools.groupby(lines, lambda z: z.lower().strip() == 'wait') if not x]
    # True or False for every execution
    outcomes = []
    for line_set in line_sets:
        with ThreadPoolExecutor(max_workers=int(max_workers)) as executor:
            futures = [loop.run_in_executor(executor, execute_line, line, tm1_services)
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
    :return: path_to_file and max_workers
    """
    # too few arguments
    if len(args) < 3:
        msg = MSG_RUSHTI_TOO_FEW_ARGUMENTS.format(app_name=APP_NAME)
        logging.error(msg)
        sys.exit(msg)
    # txt file doesnt exist
    if not os.path.isfile(args[1]):
        msg = MSG_RUSHTI_ARGUMENT1_INVALID
        logging.error(msg)
        sys.exit(msg)
    # max_workers is not a number
    if not args[2].isdigit():
        msg = MSG_RUSHTI_ARGUMENT2_INVALID
        logging.error(msg)
        sys.exit(msg)
    return args[1], args[2]


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


# receives two arguments: 1) path-to-txt-file, 2) max-workers
if __name__ == "__main__":
    logging.info(MSG_RUSHTI_STARTS.format(
        app_name=APP_NAME,
        parameters=sys.argv))
    # start timer
    start = datetime.datetime.now()
    # read commandline arguments
    path_to_file, maximum_workers = translate_cmd_arguments(*sys.argv)
    # setup connections
    tm1_service_by_instance = setup_tm1_services()
    # execution
    event_loop = asyncio.get_event_loop()
    try:
        results = event_loop.run_until_complete(
            work_through_tasks(path_to_file, maximum_workers, tm1_service_by_instance))
    finally:
        logout(tm1_service_by_instance)
        event_loop.close()
    # timing
    duration = datetime.datetime.now() - start
    exit_rushti(executions=len(results), successes=sum(results), elapsed_time=duration)
