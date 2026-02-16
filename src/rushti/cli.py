"""
RushTI CLI module.

This module provides the command-line interface for RushTI, including:
- Task execution with parallel workers
- Subcommands: run, resume, tasks (export, push, expand, visualize, validate), analyze, build
- Configuration management and settings precedence
"""

import argparse
import asyncio
import csv
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timedelta
from logging.config import fileConfig
from pathlib import Path
from typing import Optional

from TM1py.Utils import integerize_version

from rushti.utils import set_current_directory
from rushti.task import ExecutionMode
from rushti.settings import load_settings, get_effective_settings
from rushti.taskfile import (
    detect_file_type,
    parse_json_taskfile,
    TaskfileValidationError,
)
from rushti.exclusive import (
    build_session_context,
    wait_for_exclusive_access,
    ExclusiveModeTimeoutError,
)
from rushti.logging import create_execution_logger
from rushti.stats import create_stats_database
from rushti.checkpoint import (
    CheckpointManager,
    load_checkpoint,
    find_checkpoint_for_taskfile,
)
from rushti import __version__

APP_NAME = "RushTI"
CURRENT_DIRECTORY = set_current_directory()

# Track which legacy paths have been warned about (to avoid duplicate warnings)
_legacy_path_warnings = set()


def print_banner():
    """Print RushTI banner with Argentina-inspired Cubewise colors."""
    # Argentina flag true colors (24-bit ANSI)
    S = "\033[38;2;108;172;228m"  # Sky blue - celeste (#6CACE4)
    G = "\033[38;2;255;184;28m"  # Gold - sol de mayo (#FFB81C)
    W = "\033[97m"  # White
    D = "\033[90m"  # Dim gray
    R = "\033[0m"  # Reset

    banner = f"""
{S}    ██████╗ ██╗   ██╗███████╗██╗  ██╗████████╗██╗
    ██╔══██╗██║   ██║██╔════╝██║  ██║╚══██╔══╝██║
    ██████╔╝██║   ██║███████╗███████║   ██║   ██║
    ██╔══██╗██║   ██║╚════██║██╔══██║   ██║   ██║
    ██║  ██║╚██████╔╝███████║██║  ██║   ██║   ██║
    ╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚═╝{R}

       {G}⚡{W} Parallel TM1 Process Execution Engine {G}⚡{R}

    {S}[{G}P1{S}]──┐
    {S}[{G}P2{S}]──┼──▶ [{G}P4{S}]──┐
    {S}[{G}P3{S}]──┘          ├──▶ [{G}P6{S}]
               {S}[{G}P5{S}]──┘{R}

{W}                    CUBEWISE{R}
{S}                   #dogoodtm1{R}
{D}              https://cubewise.com{R}
"""
    print(banner)


def resolve_config_path(filename: str, warn_on_legacy: bool = True, cli_path: str = None) -> str:
    """Resolve configuration file path with fallback to legacy location.

    Checks for configuration files in this order:
    1. CLI argument (if provided)
    2. RUSHTI_DIR environment variable (looks in {RUSHTI_DIR}/config/)
    3. Current directory (legacy location with deprecation warning)
    4. config/ directory (recommended location)

    :param filename: Name of the configuration file (e.g., "config.ini")
    :param warn_on_legacy: Whether to log a deprecation warning if legacy path is used
    :param cli_path: Optional CLI-provided path (takes precedence)
    :return: Path to the configuration file
    """
    # 1. CLI argument takes precedence
    if cli_path:
        if os.path.exists(cli_path):
            return cli_path
        raise FileNotFoundError(f"Config file not found: {cli_path}")

    # 2. RUSHTI_DIR environment variable (config files live in config/ subdirectory)
    rushti_dir = os.environ.get("RUSHTI_DIR")
    if rushti_dir:
        env_path = os.path.join(rushti_dir, "config", filename)
        if os.path.exists(env_path):
            return env_path
        # Warn but continue to fallback
        logging.warning(
            f"RUSHTI_DIR is set to '{rushti_dir}' but '{filename}' "
            f"was not found in '{rushti_dir}/config/'"
        )

    # 3. Current directory (legacy location)
    legacy_path = os.path.join(CURRENT_DIRECTORY, filename)
    if os.path.exists(legacy_path):
        if warn_on_legacy and filename not in _legacy_path_warnings:
            _legacy_path_warnings.add(filename)
        return legacy_path

    # 4. config/ subdirectory (recommended location)
    new_path = os.path.join(CURRENT_DIRECTORY, "config", filename)
    if os.path.exists(new_path):
        return new_path

    # Neither exists - return new path for error messaging
    return new_path


def log_legacy_path_warnings(logger):
    """Log deprecation warnings for any legacy paths that were used.

    Call this after logging is initialized to emit any pending warnings.
    """
    for filename in _legacy_path_warnings:
        logger.warning(
            f"DEPRECATION: '{filename}' found in root directory. "
            f"Please move it to 'config/{filename}' or set environment variable. "
            f"Legacy path support will be removed in a future version."
        )


CONFIG = resolve_config_path("config.ini")
LOGGING_CONFIG = resolve_config_path("logging_config.ini")

from rushti.messages import (  # noqa: E402
    MSG_RUSHTI_STARTS,
    MSG_RUSHTI_WRONG_NUMBER_OF_ARGUMENTS,
    MSG_RUSHTI_ARGUMENT1_INVALID,
    MSG_RUSHTI_ARGUMENT2_INVALID,
    MSG_RUSHTI_ARGUMENT3_INVALID,
    MSG_RUSHTI_ARGUMENT4_INVALID,
    MSG_RUSHTI_ENDS,
    MSG_RUSHTI_ABORTED,
    LOG_LEVELS,
)

# Initialize logging if config exists
if os.path.isfile(LOGGING_CONFIG):
    fileConfig(LOGGING_CONFIG)

    # Fix file handler paths to use application directory
    # When fileConfig() parses the logging config, relative paths like 'rushti.log' are resolved
    # against the current working directory (where the command was invoked from), not the
    # application directory. We need to close the handler and reopen it at the correct path.
    from rushti.utils import resolve_app_path

    log_file_path = os.path.normpath(os.path.abspath(resolve_app_path("logs/rushti.log")))

    for handler in logging.root.handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            old_path = os.path.normpath(os.path.abspath(handler.baseFilename))
            # Check if the handler was opened at a different location than the app directory
            if old_path != log_file_path:
                # Close the old handler and its stream
                handler.close()

                # Try to remove the incorrectly created empty log file
                try:
                    if os.path.isfile(old_path) and os.path.getsize(old_path) == 0:
                        os.remove(old_path)
                except OSError:
                    pass  # Ignore errors - file might be in use or permissions issue

                # Update to correct path and reopen
                os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
                handler.baseFilename = log_file_path
                handler.stream = handler._open()

logger = logging.getLogger()

# Log any deprecation warnings for legacy config paths
log_legacy_path_warnings(logger)


def add_log_level_arg(parser: argparse.ArgumentParser) -> None:
    """Add --log-level argument to a parser.

    :param parser: ArgumentParser to add the argument to
    """
    parser.add_argument(
        "--log-level",
        "-L",
        dest="log_level",
        choices=LOG_LEVELS,
        default=None,
        metavar="LEVEL",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Overrides logging_config.ini",
    )


def apply_log_level(log_level: Optional[str]) -> None:
    """Apply log level override if specified.

    Updates the root logger and all its handlers to the specified level.

    :param log_level: Log level string (e.g., "DEBUG", "INFO") or None
    """
    if log_level is None:
        return

    level = getattr(logging, log_level.upper(), None)
    if level is None:
        return

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    for handler in root_logger.handlers:
        handler.setLevel(level)


from rushti.parsing import convert_json_to_dag, build_dag  # noqa: E402
from rushti.execution import (  # noqa: E402
    setup_tm1_services,
    validate_tasks,
    work_through_tasks_dag,
    logout,
)
from rushti.execution import ExecutionContext  # noqa: E402


def translate_cmd_arguments(*args):
    """Translation and Validity-checks for command line arguments.


    :param args:
    :return: tasks_file_path, max_workers, execution_mode, retries, result_file
    """
    # too few arguments
    if len(args) < 3 or len(args) > 6:
        msg = MSG_RUSHTI_WRONG_NUMBER_OF_ARGUMENTS.format(app_name=APP_NAME)
        logger.error(msg)
        sys.exit(msg)

    # default values
    mode = ExecutionMode.NORM
    retries = 0
    result_file = ""

    # txt file doesnt exist
    tasks_file = args[1]
    if not os.path.isfile(tasks_file):
        msg = MSG_RUSHTI_ARGUMENT1_INVALID
        logger.error(msg)
        sys.exit(msg)

    # max_workers is not a number
    max_workers = args[2]
    if not max_workers.isdigit():
        msg = MSG_RUSHTI_ARGUMENT2_INVALID
        logger.error(msg)
        sys.exit(msg)
    else:
        max_workers = int(max_workers)

    if len(args) >= 4:
        try:
            mode = ExecutionMode(args[3])
        except ValueError:
            msg = MSG_RUSHTI_ARGUMENT3_INVALID
            logger.error(msg)
            sys.exit(msg)

    if len(args) >= 5:
        retries = args[4]
        if not retries.isdigit():
            msg = MSG_RUSHTI_ARGUMENT4_INVALID
            logger.error(msg)
            sys.exit(msg)
        else:
            retries = int(retries)

    if len(args) >= 6:
        result_file = args[5]

    return tasks_file, max_workers, mode, retries, result_file


def add_taskfile_source_args(
    parser: argparse.ArgumentParser,
    required: bool = False,
    include_settings: bool = True,
) -> None:
    """Add taskfile source arguments to a parser.

    Adds --tasks, --tm1-instance, and --workflow arguments.

    :param parser: ArgumentParser to add arguments to
    :param required: Whether a taskfile source is required
    :param include_settings: Whether to include --settings argument
    """
    parser.add_argument(
        "--tasks",
        "-t",
        dest="taskfile_path",
        required=False,
        default=None,
        metavar="FILE",
        help="Path to the task file (JSON or TXT format)",
    )

    parser.add_argument(
        "--tm1-instance",
        dest="tm1_instance",
        metavar="INSTANCE",
        default=None,
        help="TM1 instance name to read taskfile from (requires --workflow)",
    )

    parser.add_argument(
        "--workflow",
        "-W",
        dest="workflow",
        metavar="ID",
        default=None,
        help="Workflow name (defaults to JSON metadata or taskfile filename)",
    )

    if include_settings:
        parser.add_argument(
            "--settings",
            "-s",
            dest="settings_file",
            default=None,
            metavar="FILE",
            help="Path to settings.ini file",
        )


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser for named arguments.

    :return: Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        prog=APP_NAME,
        description="Execute TM1 processes in parallel with dependency management.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --tasks tasks.txt --max-workers 4
  %(prog)s -t tasks.txt -w 4 -r 2 -o results.csv
  %(prog)s --tasks tasks.json --max-workers 8 --retries 3
  %(prog)s --tm1-instance tm1srv01 --workflow Sample --max-workers 4

Configuration:
  Default values can be set in settings.ini (see settings.ini.template).
  Settings precedence: CLI args > JSON task file > settings.ini > defaults
  File format is auto-detected (--mode is deprecated for file sources).
        """,
    )

    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version=f"{APP_NAME} {__version__}",
    )

    # Add log level argument
    add_log_level_arg(parser)

    # Add taskfile source arguments
    add_taskfile_source_args(parser, required=False, include_settings=True)

    parser.add_argument(
        "--max-workers",
        "-w",
        dest="max_workers",
        type=int,
        default=None,
        metavar="N",
        help="Maximum number of parallel workers. Default: from settings.ini or 4",
    )

    parser.add_argument(
        "--mode",
        "-m",
        dest="execution_mode",
        choices=["norm", "opt"],
        default=None,
        help="[Deprecated] Execution mode is now auto-detected from file content. This option is kept for backwards compatibility but ignored.",
    )

    parser.add_argument(
        "--retries",
        "-r",
        dest="retries",
        type=int,
        default=None,
        metavar="N",
        help="Number of retries for failed process executions. Default: from settings.ini or 0",
    )

    parser.add_argument(
        "--result",
        "-o",
        dest="output_file",
        default=None,
        metavar="FILE",
        help="Output file path for execution results (omit to skip CSV creation)",
    )

    parser.add_argument(
        "--force",
        "-f",
        dest="force",
        action="store_true",
        default=False,
        help="Bypass exclusive mode checks and run immediately (use with caution)",
    )

    parser.add_argument(
        "--exclusive",
        "-x",
        dest="exclusive",
        action="store_true",
        default=None,
        help="Run in exclusive mode - waits for other RushTI instances to complete",
    )

    # Note: --resume, --resume-from, --checkpoint removed from run command
    # Use 'rushti resume' subcommand instead to resume from checkpoint

    parser.add_argument(
        "--no-checkpoint",
        dest="no_checkpoint",
        action="store_true",
        default=False,
        help="Disable checkpoint saving for this run",
    )

    parser.add_argument(
        "--optimize",
        dest="optimize",
        choices=["longest_first", "shortest_first"],
        default=None,
        metavar="ALGORITHM",
        help=(
            "Enable task scheduling optimization with the specified algorithm. "
            "longest_first: run longest tasks first (best for independent workloads). "
            "shortest_first: run shortest tasks first (best for shared-resource TM1 workloads). "
            "Requires [stats] enabled = true in settings.ini. "
            "Overrides optimization_algorithm in JSON taskfile settings."
        ),
    )

    return parser


def parse_named_arguments(argv: list):
    """Parse command line arguments using argparse (named argument style).

    :param argv: Command line arguments (sys.argv)
    :return: Tuple of (tasks_file_path, cli_args_dict)
    """
    parser = create_argument_parser()
    args = parser.parse_args(argv[1:])  # Skip program name

    # Handle TM1 source vs file source
    tasks_file_path = None
    tm1_instance = getattr(args, "tm1_instance", None)
    workflow = getattr(args, "workflow", None)

    if tm1_instance:
        # Reading from TM1 - workflow is required
        if not workflow:
            msg = "Error: --workflow is required when using --tm1-instance"
            logger.error(msg)
            sys.exit(msg)
        # tasks_file_path can be None when reading from TM1
        tasks_file_path = args.taskfile_path if hasattr(args, "taskfile_path") else None
    else:
        # Reading from file - validate file exists
        if not args.taskfile_path:
            msg = "Error: --tasks is required (or use --tm1-instance with --workflow)"
            logger.error(msg)
            sys.exit(msg)
        if not os.path.isfile(args.taskfile_path):
            msg = MSG_RUSHTI_ARGUMENT1_INVALID
            logger.error(msg)
            sys.exit(msg)
        tasks_file_path = args.taskfile_path

    # Convert execution mode string to ExecutionMode enum if provided
    execution_mode = None
    if args.execution_mode is not None:
        execution_mode = ExecutionMode(args.execution_mode)

    # Return as dict for settings merge
    # Note: resume, resume_from, checkpoint_file are not set here - they're only
    # set by the 'resume' subcommand which calls execute_rushti() directly
    cli_args = {
        "max_workers": args.max_workers,
        "execution_mode": execution_mode,
        "retries": args.retries,
        "output_file": args.output_file,
        "settings_file": args.settings_file,
        "force": args.force,
        "exclusive": args.exclusive,
        "no_checkpoint": args.no_checkpoint,
        "optimize": args.optimize,
        "tm1_instance": tm1_instance,
        "workflow": workflow,
        "log_level": args.log_level,
    }

    return tasks_file_path, cli_args


def uses_named_arguments(argv: list) -> bool:
    """Detect if the command line uses named argument style.

    Named argument style is detected when any argument starts with '-'
    (excluding --version and -v which are handled separately).

    :param argv: Command line arguments (sys.argv)
    :return: True if named arguments are detected, False otherwise
    """
    for arg in argv[1:]:  # Skip program name
        if arg.startswith("-") and arg not in ("--version", "-v"):
            return True
    return False


def parse_arguments(argv: list):
    """Parse command line arguments using either named or positional style.

    This function provides backwards compatibility by detecting which style
    is being used and delegating to the appropriate parser.

    :param argv: Command line arguments (sys.argv)
    :return: Tuple of (tasks_file_path, cli_args_dict)
    """
    if uses_named_arguments(argv):
        return parse_named_arguments(argv)
    else:
        # Convert positional arguments to dict format
        # Note: resume options not available in positional style - use 'rushti resume' subcommand
        tasks_file, max_workers, mode, retries, result_file = translate_cmd_arguments(*argv)
        cli_args = {
            "max_workers": max_workers,
            "execution_mode": mode,
            "retries": retries,
            "result_file": result_file,
            "settings_file": None,
            "force": False,
            "exclusive": None,
            "no_checkpoint": False,
            "log_level": None,  # Not supported in positional style
        }
        return tasks_file, cli_args


def create_results_file(
    result_file: str,
    overall_success: bool,
    executions: int,
    fails: int,
    start_time: datetime,
    end_time: datetime,
    elapsed_time: timedelta,
):
    header = (
        "PID",
        "Process Runs",
        "Process Fails",
        "Start",
        "End",
        "Runtime",
        "Overall Success",
    )
    record = (
        os.getpid(),
        executions,
        fails,
        start_time,
        end_time,
        elapsed_time,
        overall_success,
    )

    Path(result_file).parent.mkdir(parents=True, exist_ok=True)
    with open(result_file, "w", encoding="utf-8") as file:
        cw = csv.writer(file, delimiter="|", lineterminator="\n")
        cw.writerows([header, record])


def exit_rushti(
    overall_success: bool,
    executions: int,
    successes: int,
    start_time: datetime,
    end_time: datetime,
    elapsed_time: timedelta,
    result_file: str,
):
    """Exit RushTI with exit code 0 or 1 depending on the TI execution outcomes
    :param overall_success: Exception raised during executions
    :param executions: Number of executions
    :param successes: Number of executions that succeeded
    :param start_time:
    :param end_time:
    :param elapsed_time:
    :param result_file:
    :return:
    """
    if not overall_success:
        message = MSG_RUSHTI_ABORTED.format(app_name=APP_NAME)
        logger.error(message)
        sys.exit(message)

    fails = executions - successes
    message = MSG_RUSHTI_ENDS.format(
        app_name=APP_NAME,
        fails=fails,
        executions=executions,
        time=str(elapsed_time),
        parameters=sys.argv,
    )

    if result_file:
        create_results_file(
            result_file,
            overall_success,
            executions,
            fails,
            start_time,
            end_time,
            elapsed_time,
        )

    if fails > 0:
        logger.error(message)
        sys.exit(message)

    logger.info(message)
    sys.exit(0)


from rushti.commands import (  # noqa: E402
    run_build_command,
    run_resume_command,
    run_tasks_command,
    run_stats_command,
    run_db_command,
)


def main() -> int:
    """Main entry point for RushTI CLI.

    :return: Exit code (0 for success, non-zero for failure)
    """
    # Configure stdout/stderr to use UTF-8 encoding on Windows
    # This fixes Unicode display issues with the banner and other output
    if sys.platform == "win32":
        import codecs

        if hasattr(sys.stdout, "buffer"):
            sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
        if hasattr(sys.stderr, "buffer"):
            sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

    # Handle --version flag for positional style (named style handles it via argparse)
    if len(sys.argv) == 2 and sys.argv[1] in ("--version", "-v"):
        print_banner()
        print(f"{APP_NAME} {__version__}")
        return 0

    # Dispatch to subcommand handlers
    SUBCOMMANDS = {
        "build": run_build_command,
        "resume": run_resume_command,
        "tasks": run_tasks_command,
        "stats": run_stats_command,
        "db": run_db_command,
    }

    resume_context = None
    if len(sys.argv) >= 2 and sys.argv[1] in SUBCOMMANDS:
        subcommand = sys.argv[1]
        result = SUBCOMMANDS[subcommand](sys.argv)
        # Most subcommand handlers call sys.exit()
        # The 'resume' handler returns a context dict for main() to merge
        if subcommand != "resume":
            return 0
        resume_context = result

    # Handle explicit "run" command by stripping it from argv
    if len(sys.argv) >= 2 and sys.argv[1] == "run":
        # If 'rushti run -h' or 'rushti run --help', show run-specific help
        if len(sys.argv) == 3 and sys.argv[2] in ("--help", "-h"):
            print_banner()
            parser = create_argument_parser()
            parser.prog = f"{APP_NAME} run"
            parser.print_help()
            return 0
        sys.argv = [sys.argv[0]] + sys.argv[2:]

    # Handle --help with no subcommand (top-level overview)
    if len(sys.argv) == 2 and sys.argv[1] in ("--help", "-h"):
        print_banner()
        print(f"""\
{APP_NAME} {__version__} - Parallel TI Process Execution

Usage:
  {APP_NAME} [command] [options]

Commands:
  run           Execute task file (default if no command specified)
  resume        Resume execution from checkpoint
  tasks         Taskfile operations (export, push, expand, visualize, validate)
  stats         Query and analyze execution statistics
  build         Create TM1 logging objects
  db            Database administration (clear, vacuum, list taskfiles)

Quick Start:
  {APP_NAME} --tasks tasks.txt --max-workers 4
  {APP_NAME} run --tasks tasks.json --max-workers 8 --retries 3
  {APP_NAME} run --tm1-instance tm1srv01 --workflow DailyETL --max-workers 4

Use '{APP_NAME} <command> --help' for command-specific options and examples.
""")
        return 0

    # Default behavior: execute task file (run mode)
    logger.info(MSG_RUSHTI_STARTS.format(app_name=APP_NAME, parameters=sys.argv))
    # start timer
    start = datetime.now()

    # read commandline arguments (supports both positional and named styles)
    tasks_file_path, cli_args = parse_arguments(sys.argv)

    # Merge resume context if set by resume subcommand
    # This allows resume-specific args to flow through without being CLI-parseable
    if resume_context:
        cli_args.update(resume_context)

    # Apply log level override if specified
    apply_log_level(cli_args.get("log_level"))

    # Load settings from settings.ini
    settings = load_settings(cli_args.get("settings_file"))

    # Apply CLI overrides to settings
    settings = get_effective_settings(settings, cli_args=cli_args)

    # Extract final values (CLI overrides settings.ini)
    max_workers = (
        cli_args["max_workers"]
        if cli_args.get("max_workers") is not None
        else settings.defaults.max_workers
    )
    process_execution_retries = (
        cli_args["retries"] if cli_args.get("retries") is not None else settings.defaults.retries
    )
    result_file = (
        cli_args["output_file"]
        if cli_args.get("output_file") is not None
        else settings.defaults.result_file
    )
    # Resolve result file path relative to application directory
    from rushti.utils import resolve_app_path

    result_file = resolve_app_path(result_file)

    logger.debug(
        f"Effective settings: max_workers={max_workers}, retries={process_execution_retries}, result_file={result_file}"
    )

    # Handle TM1 source if specified
    tm1_instance = cli_args.get("tm1_instance")
    workflow = cli_args.get("workflow")
    tm1_taskfile = None  # Will hold Taskfile object if reading from TM1

    if tm1_instance:
        # Read taskfile from TM1 cube
        # workflow is validated as non-None in parse_arguments when tm1_instance is set
        assert workflow is not None, "workflow must be provided with tm1_instance"
        logger.info(f"Reading taskfile from TM1 instance '{tm1_instance}', workflow '{workflow}'")
        try:
            from rushti.tm1_integration import read_taskfile_from_tm1, connect_to_tm1_instance

            tm1_source = connect_to_tm1_instance(tm1_instance, CONFIG)
            try:
                tm1_taskfile = read_taskfile_from_tm1(
                    tm1_source,
                    workflow,
                    cube_name=settings.tm1_integration.default_rushti_cube,
                    dim_workflow=settings.tm1_integration.default_workflow_dim,
                    dim_task=settings.tm1_integration.default_task_id_dim,
                    dim_run=settings.tm1_integration.default_run_id_dim,
                    dim_measure=settings.tm1_integration.default_measure_dim,
                )
                logger.info(f"Loaded {len(tm1_taskfile.tasks)} tasks from TM1")
            finally:
                tm1_source.logout()
        except Exception as e:
            logger.error(f"Failed to read taskfile from TM1: {e}")
            sys.exit(1)

    # Determine workflow and exclusive mode from task file
    # Parse task file early to get metadata for session context
    if tm1_taskfile:
        # Using TM1 source
        file_type = "json"  # TM1 taskfiles are treated as JSON format
        # workflow already set from cli_args above - validated in parse_arguments
        assert workflow is not None, "workflow must be set when using TM1 source"
        exclusive_mode = cli_args.get("exclusive")  # CLI override
    else:
        # Using file source
        file_type = detect_file_type(tasks_file_path)
        # Workflow precedence: CLI --workflow > JSON metadata > filename stem
        workflow = cli_args.get("workflow") or Path(tasks_file_path).stem
        exclusive_mode = cli_args.get("exclusive")  # CLI override

    if file_type == "json" and not tm1_taskfile:
        # Quick parse to get metadata and settings
        try:
            taskfile_preview = parse_json_taskfile(tasks_file_path)
            if taskfile_preview.metadata.workflow and not cli_args.get("workflow"):
                workflow = taskfile_preview.metadata.workflow
            # Apply exclusive from JSON if not set via CLI
            if exclusive_mode is None and taskfile_preview.settings.exclusive is not None:
                exclusive_mode = taskfile_preview.settings.exclusive
        except TaskfileValidationError as e:
            logger.error(str(e))
            sys.exit(1)

    # Apply exclusive from settings.ini if not set via CLI or JSON
    if exclusive_mode is None:
        exclusive_mode = settings.exclusive_mode.enabled

    force_mode = cli_args.get("force", False)

    logger.debug(f"Exclusive mode: {exclusive_mode}, Force: {force_mode}, Workflow: {workflow}")

    # setup connections with session context for RushTI identification
    if tm1_taskfile:
        # When reading from TM1, extract instances from the taskfile object
        tm1_instances_needed = set(task.instance for task in tm1_taskfile.tasks)
        # Create a dummy path for logging purposes
        tasks_file_path_for_services = f"TM1:{tm1_instance}/{workflow}"
    else:
        tm1_instances_needed = None
        tasks_file_path_for_services = tasks_file_path

    # Build session context string for exclusive mode checks
    session_context = build_session_context(workflow, exclusive_mode)
    logger.debug(f"Session context: {session_context}")

    tm1_service_by_instance, preserve_connections = setup_tm1_services(
        max_workers=max_workers,
        tasks_file_path=tasks_file_path_for_services if not tm1_taskfile else None,
        workflow=workflow,
        exclusive=exclusive_mode,
        tm1_instances=tm1_instances_needed,
    )

    # Always check for exclusive access before execution
    # - If current run is exclusive (-x flag): wait for ANY RushTI session
    # - If current run is non-exclusive: wait only for EXCLUSIVE sessions
    # This ensures exclusive runs are respected by all other runs
    logger.debug(f"Exclusive access check: exclusive_mode={exclusive_mode}")
    logger.debug(f"TM1 services available: {list(tm1_service_by_instance.keys())}")
    try:
        wait_for_exclusive_access(
            tm1_services=tm1_service_by_instance,
            current_exclusive=exclusive_mode,
            session_context=session_context,
            polling_interval=settings.exclusive_mode.polling_interval,
            timeout=settings.exclusive_mode.timeout,
            force=force_mode,
        )
        logger.debug("Exclusive access check completed successfully")
    except ExclusiveModeTimeoutError as e:
        logger.error(str(e))
        # Force logout in exclusive mode to ensure session context is cleared
        logout(tm1_service_by_instance, preserve_connections, force=exclusive_mode)
        sys.exit(1)

    # setup results variable (guarantee it's not empty in case of error)
    results = list()

    # Initialize stats database if enabled
    from rushti.stats import get_db_path

    db_kwargs = dict(
        enabled=settings.stats.enabled,
        retention_days=settings.stats.retention_days,
        db_path=get_db_path(settings),
    )
    ctx = ExecutionContext(
        stats_db=create_stats_database(**db_kwargs),
        execution_logger=create_execution_logger(workflow=workflow),
    )
    if settings.stats.enabled:
        logger.debug("Stats database initialized")
    logger.debug(f"Execution logger initialized: run_id={ctx.execution_logger.run_id}")

    # spawn event loop for parallelization
    event_loop = None
    checkpoint_manager = None
    results = []  # Initialize to avoid NameError if exception occurs before assignment

    # Determine checkpoint settings
    resume_mode = cli_args.get("resume", False)
    resume_from_task = cli_args.get("resume_from")
    checkpoint_file = cli_args.get("checkpoint_file")
    no_checkpoint = cli_args.get("no_checkpoint", False)
    checkpoint_enabled = (settings.resume.enabled or resume_mode) and not no_checkpoint

    try:
        # Build DAG from tasks source
        if tm1_taskfile:
            # Use TM1 taskfile directly - convert to DAG
            dag = convert_json_to_dag(
                tm1_taskfile, expand=False, tm1_services=tm1_service_by_instance
            )
            dag.validate()
            taskfile = tm1_taskfile
            # Apply settings from TM1 taskfile (CLI still overrides)
            if taskfile.settings.max_workers and cli_args.get("max_workers") is None:
                max_workers = taskfile.settings.max_workers
            if taskfile.settings.retries is not None and cli_args.get("retries") is None:
                process_execution_retries = taskfile.settings.retries
            if taskfile.settings.result_file and cli_args.get("output_file") is None:
                result_file = resolve_app_path(taskfile.settings.result_file)
            logger.debug(
                f"Applied TM1 taskfile settings: max_workers={max_workers}, retries={process_execution_retries}"
            )
        else:
            # Build DAG from file with dependency validation
            dag_result = build_dag(
                tasks_file_path,
                expand=True,
                tm1_services=tm1_service_by_instance,
            )

            # Handle JSON files which return (DAG, Taskfile) tuple
            taskfile = None
            if isinstance(dag_result, tuple):
                dag, taskfile = dag_result
                # Apply JSON settings (CLI still overrides)
                if taskfile.settings.max_workers and cli_args.get("max_workers") is None:
                    max_workers = taskfile.settings.max_workers
                if taskfile.settings.retries is not None and cli_args.get("retries") is None:
                    process_execution_retries = taskfile.settings.retries
                if taskfile.settings.result_file and cli_args.get("output_file") is None:
                    result_file = resolve_app_path(taskfile.settings.result_file)
                logger.debug(
                    f"Applied JSON settings: max_workers={max_workers}, retries={process_execution_retries}"
                )
            else:
                dag = dag_result
                # TXT source: convert to Taskfile for archiving
                from rushti.taskfile import convert_txt_to_json

                taskfile = convert_txt_to_json(tasks_file_path)

        # Archive taskfile as JSON for historical DAG reconstruction
        from rushti.taskfile import archive_taskfile

        run_id = ctx.execution_logger.run_id
        archived_taskfile_path = archive_taskfile(taskfile, workflow, run_id)

        # Get all tasks for checkpoint management
        all_tasks = dag.get_all_tasks()
        all_task_ids = [str(t.id) for t in all_tasks]

        # Start stats run if enabled (clear any leftover data first)
        if ctx.stats_db and ctx.stats_db.enabled:
            with ctx.stats_data_lock:
                ctx.stats_data.clear()
            effective_taskfile_path = archived_taskfile_path
            # Determine algorithm for stats recording
            # (at this point 'algorithm' is not yet resolved; compute it here)
            _stats_algorithm = cli_args.get("optimize") or (
                taskfile.settings.optimization_algorithm if taskfile else None
            )
            ctx.stats_db.start_run(
                run_id=ctx.execution_logger.run_id,
                workflow=workflow,
                taskfile_path=effective_taskfile_path,
                task_count=len(all_tasks),
                # Taskfile metadata (from JSON file if available)
                taskfile_name=taskfile.metadata.name if taskfile else None,
                taskfile_description=taskfile.metadata.description if taskfile else None,
                taskfile_author=taskfile.metadata.author if taskfile else None,
                # Effective settings
                max_workers=max_workers,
                retries=process_execution_retries,
                result_file=result_file,
                exclusive=exclusive_mode,
                optimize=bool(_stats_algorithm),
                optimization_algorithm=_stats_algorithm,
            )

        # Handle resume mode
        completed_task_ids = set()
        skipped_task_ids = set()

        if resume_mode or checkpoint_file:
            # Load existing checkpoint
            if checkpoint_file:
                loaded_checkpoint = load_checkpoint(checkpoint_file)
            else:
                # Try to find checkpoint for this taskfile
                found_checkpoint = find_checkpoint_for_taskfile(
                    settings.resume.checkpoint_dir,
                    tasks_file_path,
                )
                if found_checkpoint:
                    loaded_checkpoint = load_checkpoint(found_checkpoint)
                else:
                    loaded_checkpoint = None
                    if resume_mode:
                        logger.warning(f"No checkpoint found for {tasks_file_path}, starting fresh")

            if loaded_checkpoint:
                # Validate checkpoint matches current taskfile
                is_valid, warnings = loaded_checkpoint.validate_against_taskfile(
                    tasks_file_path,
                    strict=not force_mode,
                )
                for warning in warnings:
                    logger.warning(warning)

                if not is_valid and not force_mode:
                    raise ValueError("Checkpoint validation failed. Use --force to override.")

                if resume_from_task:
                    # Resume from specific task, marking earlier tasks as skipped
                    tasks_to_run = loaded_checkpoint.get_resume_from_task(
                        resume_from_task,
                        all_task_ids,
                    )
                    # Mark tasks not in tasks_to_run and not already completed as skipped
                    for task_id in all_task_ids:
                        if task_id in loaded_checkpoint.completed_tasks:
                            completed_task_ids.add(task_id)
                        elif task_id not in tasks_to_run:
                            skipped_task_ids.add(task_id)
                    logger.info(
                        f"Resuming from task {resume_from_task}: "
                        f"{len(completed_task_ids)} completed, {len(skipped_task_ids)} skipped"
                    )
                else:
                    # Automatic resume - check safe_retry for in-progress tasks
                    task_safe_retry_map = {
                        str(t.id): getattr(t, "safe_retry", False) for t in all_tasks
                    }
                    tasks_to_run, tasks_requiring_decision, error_message = (
                        loaded_checkpoint.get_tasks_for_resume(task_safe_retry_map)
                    )

                    if error_message:
                        raise ValueError(error_message)

                    # Mark completed tasks
                    completed_task_ids = set(loaded_checkpoint.completed_tasks.keys())
                    logger.info(
                        f"Resuming execution: {len(completed_task_ids)} completed, "
                        f"{len(tasks_to_run)} to run"
                    )

                # Mark completed tasks in DAG and ctx.task_execution_results
                for task_id in completed_task_ids:
                    result = loaded_checkpoint.completed_tasks.get(task_id)
                    if result:
                        dag.mark_complete(task_id, result.success)
                        # Also update ctx.task_execution_results for verify_predecessors_ok()
                        ctx.task_execution_results[task_id] = result.success

                # Mark skipped tasks in DAG and ctx.task_execution_results
                for task_id in skipped_task_ids:
                    dag.mark_complete(task_id, False)
                    ctx.task_execution_results[task_id] = False

        # Validate tasks against TM1 instances
        if not validate_tasks(all_tasks, tm1_service_by_instance):
            raise ValueError("Invalid tasks provided")

        # Initialize checkpoint manager
        if checkpoint_enabled:
            checkpoint_manager = CheckpointManager(
                checkpoint_dir=settings.resume.checkpoint_dir,
                taskfile_path=tasks_file_path,
                workflow=workflow,
                task_ids=all_task_ids,
                checkpoint_interval=settings.resume.checkpoint_interval,
                enabled=True,
            )
            # If resuming, update checkpoint with already-completed tasks
            if completed_task_ids and checkpoint_manager.checkpoint:
                for task_id in completed_task_ids:
                    if task_id in loaded_checkpoint.completed_tasks:
                        result = loaded_checkpoint.completed_tasks[task_id]
                        checkpoint_manager.checkpoint.completed_tasks[task_id] = result
                        checkpoint_manager.checkpoint.pending_tasks.discard(task_id)
                for task_id in skipped_task_ids:
                    checkpoint_manager.checkpoint.mark_skipped(task_id, "resume_from_task")
            logger.info(f"Checkpoint manager initialized: dir={settings.resume.checkpoint_dir}")

        # Initialize task optimizer if an algorithm is specified
        # Precedence: CLI --optimize > JSON optimization_algorithm > no optimization
        task_optimizer = None
        algorithm = cli_args.get("optimize") or (
            taskfile.settings.optimization_algorithm if taskfile else None
        )

        if algorithm:
            if ctx.stats_db and ctx.stats_db.enabled:
                from rushti.optimizer import create_task_optimizer

                task_optimizer = create_task_optimizer(
                    stats_db=ctx.stats_db,
                    settings=settings.optimization,
                    workflow=workflow,
                    algorithm=algorithm,
                )
                if task_optimizer:
                    # Build runtime estimate cache for all tasks
                    task_optimizer.build_cache(all_tasks)
                    logger.info(
                        f"Task optimization enabled (algorithm={algorithm}, "
                        f"lookback={settings.optimization.lookback_runs}, "
                        f"min_samples={settings.optimization.min_samples})"
                    )
            else:
                logger.warning(
                    f"Optimization algorithm '{algorithm}' requested but stats database "
                    "is disabled. Enable [stats] enabled = true to use optimization."
                )

        # Execute using DAG-based scheduler
        event_loop = asyncio.new_event_loop()
        results = event_loop.run_until_complete(
            work_through_tasks_dag(
                ctx,
                dag,
                max_workers,
                process_execution_retries,
                tm1_service_by_instance,
                checkpoint_manager=checkpoint_manager,
                task_optimizer=task_optimizer,
            )
        )
        success = True

    except Exception:
        logging.exception("Fatal Error")
        success = False

    finally:
        # Force logout in exclusive mode to ensure session context is cleared
        # This allows subsequent exclusive runs to proceed without waiting
        logout(tm1_service_by_instance, preserve_connections, force=exclusive_mode)
        if event_loop:
            event_loop.close()
        # Clean up checkpoint
        if checkpoint_manager:
            checkpoint_manager.cleanup(success=success)

        # Batch write stats data and complete run (in finally to ensure cleanup)
        if ctx.stats_db and ctx.stats_db.enabled:
            try:
                # Batch write all collected task stats (avoids SQLite concurrency issues)
                with ctx.stats_data_lock:
                    if ctx.stats_data:
                        ctx.stats_db.batch_record_tasks(ctx.stats_data)
                        ctx.stats_data.clear()

                success_count = sum(results) if results else 0
                failure_count = len(results) - success_count if results else 0
                run_status = (
                    "Success" if success else ("Partial" if success_count > 0 else "Failed")
                )
                ctx.stats_db.complete_run(
                    run_id=ctx.execution_logger.run_id if ctx.execution_logger else "",
                    status=run_status,
                    success_count=success_count,
                    failure_count=failure_count,
                )
            except Exception as e:
                logger.error(f"Failed to complete stats run: {e}")

            # Auto-upload results to TM1 if push_results is enabled
            if settings.tm1_integration.push_results:
                try:
                    from rushti.tm1_integration import (
                        upload_results_to_tm1,
                        connect_to_tm1_instance,
                        build_results_dataframe,
                    )

                    tm1_instance = settings.tm1_integration.default_tm1_instance
                    if tm1_instance:
                        tm1_upload = connect_to_tm1_instance(
                            tm1_instance,
                            CONFIG,
                        )
                        try:
                            results_df = build_results_dataframe(
                                ctx.stats_db,
                                workflow,
                                ctx.execution_logger.run_id if ctx.execution_logger else "",
                            )
                            if not results_df.empty:
                                file_name = upload_results_to_tm1(
                                    tm1_upload,
                                    workflow,
                                    ctx.execution_logger.run_id if ctx.execution_logger else "",
                                    results_df,
                                )

                                logger.info(
                                    f"Results uploaded to TM1: Applications/rushti/{workflow}/"
                                    f"{ctx.execution_logger.run_id if ctx.execution_logger else ''}.log"
                                )

                                # Optionally call }rushti.load.results to load CSV into cube
                                if settings.tm1_integration.auto_load_results:
                                    # include .blb file extension for TM1 versions < 12 for process source files
                                    if integerize_version(tm1_upload.version, 2) < 12:
                                        file_name = file_name + ".blb"

                                    success, status, error_log_file = (
                                        tm1_upload.processes.execute_with_return(
                                            "}rushti.load.results",
                                            pSourceFile=file_name,
                                            pTargetCube=settings.tm1_integration.default_rushti_cube,
                                        )
                                    )
                                    logger.info(
                                        "Executed }rushti.load.results on %s",
                                        tm1_instance,
                                    )
                                    if not success:
                                        logger.warning(
                                            "auto_load_results: Failed to execute "
                                            "}rushti.load.results on %s: %s",
                                            tm1_instance,
                                            status,
                                        )
                        finally:
                            tm1_upload.logout()
                    else:
                        logger.warning(
                            "push_results enabled but default_tm1_instance not configured"
                        )
                except Exception as e:
                    logger.warning(f"Failed to upload results to TM1 (non-blocking): {e}")

            # Close stats database after upload
            ctx.stats_db.close()

    # Flush execution logs to configured destinations
    if ctx.execution_logger and ctx.execution_logger.log_count > 0:
        logger.debug(f"Flushing {ctx.execution_logger.log_count} execution logs")
        flush_success = ctx.execution_logger.flush()
        if not flush_success:
            logger.warning("Some log destinations failed to receive execution logs")

    # timing
    end = datetime.now()
    duration = end - start
    exit_rushti(
        overall_success=success,
        executions=len(results),
        successes=sum(results),
        start_time=start,
        end_time=end,
        elapsed_time=duration,
        result_file=result_file,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
