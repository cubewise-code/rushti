"""Shared message constants and configuration values for the RushTI CLI.

Centralizes user-facing log/error message templates, boolean-truthy values,
and valid log-level names so they can be reused across submodules without
circular imports.
"""

# ---------------------------------------------------------------------------
# Message templates
# ---------------------------------------------------------------------------

MSG_RUSHTI_STARTS = "{app_name} starts. Parameters: {parameters}."
MSG_RUSHTI_WRONG_NUMBER_OF_ARGUMENTS = "{app_name} needs to be executed with two to four arguments."
MSG_RUSHTI_ARGUMENT1_INVALID = "Argument 1 (path to tasks file) invalid. File needs to exist."
MSG_RUSHTI_ARGUMENT2_INVALID = (
    "Argument 2 (maximum workers) invalid. Argument must be an integer number."
)
MSG_RUSHTI_ARGUMENT3_INVALID = (
    "Argument 3 (tasks file type) invalid. Argument can be 'opt' or 'norm'."
)
MSG_RUSHTI_ARGUMENT4_INVALID = "Argument 4 (retries) invalid. Argument must be an integer number."
MSG_PROCESS_EXECUTE = "Executing process: '{process_name}' with parameters: {parameters} on instance: '{instance_name}'"
MSG_PROCESS_SUCCESS = (
    "Execution successful: Process '{process}' with parameters: {parameters} with {retries} retries on instance: "
    "{instance}. Elapsed time: {time}"
)
MSG_PROCESS_FAIL_INSTANCE_NOT_IN_CONFIG_FILE = (
    "Process '{process_name}' not executed on '{instance_name}'. "
    "'{instance_name}' not defined in provided config file. Check for typos and miscapitalization."
)
MSG_PROCESS_FAIL_WITH_ERROR_FILE = (
    "Execution failed. Process: '{process}' with parameters: {parameters} with {retries} retries and status: "
    "{status}, on instance: '{instance}'. Elapsed time : {time}. Error file: {error_file}"
)
MSG_PROCESS_HAS_MINOR_ERRORS = (
    "Execution ended with minor errors but it was forced to succeed. Process: '{process}' with parameters: {parameters} with {retries} retries and status: "
    "{status}, on instance: '{instance}'. Error file: {error_file}"
)
MSG_PROCESS_FAIL_UNEXPECTED = (
    "Execution failed. Process: '{process}' with parameters: {parameters}. "
    "Elapsed time: {time}. Error: {error}."
)
MSG_RUSHTI_ENDS = (
    "{app_name} ends. {fails} fails out of {executions} executions. "
    "Elapsed time: {time}. Ran with parameters: {parameters}"
)
MSG_RUSHTI_ABORTED = "{app_name} aborted with error"
MSG_PROCESS_ABORTED_FAILED_PREDECESSOR = (
    "Execution aborted. Process: '{process}' with parameters: {parameters} is not run "
    "due to failed predecessor {predecessor}, on instance: '{instance}'"
)
MSG_PROCESS_ABORTED_UNCOMPLETE_PREDECESSOR = (
    "Execution aborted. Process: '{process}' with parameters: {parameters} is not run "
    "due to uncompleted predecessor {predecessor}, on instance: '{instance}'"
)
MSG_PROCESS_NOT_EXISTS = (
    "Task validation failed. Process: '{process}' does not exist on instance: '{instance}'"
)
MSG_PROCESS_PARAMS_INCORRECT = (
    "Task validation failed. Process: '{process}' does not have: {parameters}, "
    "on instance: '{instance}'"
)
MSG_PROCESS_TIMEOUT = (
    "Execution timed out. Process: '{process}' with parameters: {parameters} exceeded timeout of "
    "{timeout} seconds on instance: '{instance}'. Elapsed time: {time}"
)

# ---------------------------------------------------------------------------
# CLI configuration values
# ---------------------------------------------------------------------------

TRUE_VALUES = ["1", "y", "yes", "true", "t"]

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
