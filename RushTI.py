import asyncio
import configparser
import logging
import os
import shlex
import sys
import datetime
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor

from TM1py import TM1Service

#Set Current directory to the script path.
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

APPNAME = "RushTI"
LOGFILE = "{current_directory}/RushTI.log".format(current_directory=sys.path[0])
CONFIG = "{current_directory}/config.ini".format(current_directory=sys.path[0])

logging.basicConfig(
    filename=LOGFILE,
    format='%(asctime)s - RushTI - %(levelname)s - %(message)s',
    level=logging.INFO)

start = datetime.datetime.now()

def setup_tm1_services():
    """ Return Dictionary with TM1ServerName (as in config.ini) : Instantiated TM1Service
    
    :return: Dictionary server_names and TM1py.TM1Service instances pairs
    """
    if not os.path.isfile(CONFIG):
        raise ValueError("config.ini does not exist.")
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
                tm1_services[tm1_server_name] = TM1Service(**params, session_context=APPNAME)
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
        # if instance or process needs to be case insensitive
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
        return
    instance_name, process_name, parameters = extract_info_from_line(line)
    if instance_name not in tm1_services:
        msg = "Process {process_name} not executed on {instance_name}. {instance_name} not accessible.".format(
            process_name=process_name,
            instance_name=instance_name)
        logging.error(msg)
        return
    tm1 = tm1_services[instance_name]
    # Execute it
    try:
        msg = "Executing process: {process_name} with Parameters: {parameters} on instance: {instance_name}".format(
            process_name=process_name,
            parameters=parameters,
            instance_name=instance_name)
        logging.info(msg)
        tm1.processes.execute(process_name=process_name, **parameters)
        msg = "Execution Successful: {process_name} with Parameters: {parameters} on instance: {instance_name}".format(
            process_name=process_name,
            parameters=parameters,
            instance_name=instance_name)
        logging.info(msg)
    except Exception as e:
        msg = "Execution Failed. Process: {process}, Parameters: {parameters}, Error: {error}".format(
            process=process_name,
            parameters=parameters,
            error=str(e))
        logging.error(msg)


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
    with ThreadPoolExecutor(max_workers=int(max_workers)) as executor:
        futures = [loop.run_in_executor(executor, execute_line, line, tm1_services)
                   for line
                   in lines]
        for future in futures:
            await future


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
        msg = "RushTI needs to executed with two arguments."
        logging.error(msg)
        raise ValueError(msg)
    # txt file doesnt exist
    if not os.path.isfile(args[1]):
        msg = "Argument 1 (path to file) invalid. File needs to exist."
        logging.error(msg)
        raise ValueError(msg)
    # max_workers is not a number
    if not args[2].isdigit():
        msg = "Argument 2 (max workers) invalid. Needs to be number."
        logging.error(msg)
        raise ValueError(msg)
    return args[1], args[2]


# recieves two arguments: 1) path-to-txt-file, 2) max-workers
if __name__ == "__main__":
    logging.info("{app_name} starts. Parameters: {parameters}.".format(
        app_name=APPNAME,
        parameters=sys.argv))
    # read commandline arguments
    path_to_file, max_workers = translate_cmd_arguments(*sys.argv)
    # setup connections
    tm1_services = setup_tm1_services()
    # execution
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(work_through_tasks(path_to_file, max_workers, tm1_services))
    finally:
        logout(tm1_services)
        loop.close()
		
end = datetime.datetime.now()
duration = (end-start)
	
logging.info(("{app_name} ends with the duration "+str(end-start)).format(app_name=APPNAME))
