"""rushti tasks — taskfile operations dispatcher.

Subcommands:
    export:    Convert TXT taskfiles or TM1 taskfiles to JSON format.
    push:      Upload JSON taskfiles to TM1 as files for archival.
    expand:    Expand MDX expressions in taskfile parameters.
    visualize: Generate interactive HTML DAG visualization.
    validate:  Validate taskfile structure and TM1 connectivity.
"""

import argparse

from rushti.app_paths import resolve_config_path
from rushti.commands.tasks.expand import handle_tasks_expand
from rushti.commands.tasks.export import handle_tasks_export
from rushti.commands.tasks.push import handle_tasks_push
from rushti.commands.tasks.validate import handle_tasks_validate
from rushti.commands.tasks.visualize import handle_tasks_visualize
from rushti.logging_setup import add_log_level_arg, apply_log_level

APP_NAME = "RushTI"


def run_tasks_command(argv: list) -> None:
    """Execute the tasks command with subcommands for taskfile operations.

    Subcommands:
        export:    Convert TXT taskfiles or TM1 taskfiles to JSON format.
        push:      Upload JSON taskfiles to TM1 as files for archival.
        expand:    Expand MDX expressions in taskfile parameters.
        visualize: Generate interactive HTML DAG visualization.
        validate:  Validate taskfile structure and TM1 connectivity.

    Usage:
        # Export TXT to JSON
        rushti tasks export --tasks tasks.txt --output tasks.json

        # Export from TM1 to JSON
        rushti tasks export --tm1-instance tm1srv01 -T DailyETL --output daily.json

        # Expand MDX expressions in taskfile
        rushti tasks expand --tasks tasks.json --output expanded.json

        # Push JSON to TM1 as file
        rushti tasks push --tasks tasks.json --tm1-instance tm1srv01

        # Visualize taskfile as DAG
        rushti tasks visualize --tasks tasks.json --output dag.html

        # Validate taskfile
        rushti tasks validate --tasks tasks.json --skip-tm1-check

    :param argv: Command line arguments
    """
    from rushti.cli import add_taskfile_source_args

    parser = argparse.ArgumentParser(
        prog=f"{APP_NAME} tasks",
        description="Taskfile operations: export, push, expand, visualize, and validate.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  {APP_NAME} tasks export --tasks tasks.txt --output tasks.json
  {APP_NAME} tasks export --tm1-instance tm1srv01 -W DailyETL --output daily.json
  {APP_NAME} tasks push --tasks tasks.json --tm1-instance tm1srv01
  {APP_NAME} tasks expand --tasks tasks.json --output expanded.json
  {APP_NAME} tasks visualize --tasks tasks.json --output dag.html
  {APP_NAME} tasks validate --tasks tasks.json --skip-tm1-check
        """,
    )

    subparsers = parser.add_subparsers(
        dest="subcommand",
        required=True,
        help="Subcommand to execute",
    )

    # --- export subcommand ---
    export_parser = subparsers.add_parser(
        "export",
        help="Export taskfile to JSON format (from TXT or TM1)",
    )
    add_taskfile_source_args(export_parser, required=False, include_settings=True)
    export_parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        required=True,
        metavar="FILE",
        help="Output JSON file path",
    )
    export_parser.add_argument(
        "--mode",
        "-m",
        dest="mode",
        choices=["norm", "opt"],
        default="opt",
        help="[TM1 sources only] 'norm' for wait-based sequencing, 'opt' for explicit predecessors. "
        "Ignored for file sources (auto-detected). Default: opt",
    )
    add_log_level_arg(export_parser)

    # --- push subcommand ---
    push_parser = subparsers.add_parser(
        "push",
        help="Push JSON taskfile to TM1 as a file",
    )
    add_taskfile_source_args(push_parser, required=False, include_settings=True)
    push_parser.add_argument(
        "--target-tm1-instance",
        dest="target_tm1_instance",
        default=None,
        metavar="INSTANCE",
        help="Target TM1 instance for push (defaults to --tm1-instance if loading from TM1)",
    )
    push_parser.add_argument(
        "--mode",
        "-m",
        dest="mode",
        choices=["norm", "opt"],
        default="opt",
        help="[TM1 sources only] 'norm' for wait-based sequencing, 'opt' for explicit predecessors. "
        "Ignored for file sources (auto-detected). Default: opt",
    )
    add_log_level_arg(push_parser)

    # --- expand subcommand ---
    expand_parser = subparsers.add_parser(
        "expand",
        help="Expand MDX expressions in taskfile parameters",
    )
    add_taskfile_source_args(expand_parser, required=False, include_settings=True)
    expand_parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        required=True,
        metavar="FILE",
        help="Output file path",
    )
    expand_parser.add_argument(
        "--format",
        "-f",
        dest="output_format",
        choices=["json", "txt"],
        default=None,
        help="Output format (default: inferred from output extension)",
    )
    expand_parser.add_argument(
        "--mode",
        "-m",
        dest="mode",
        choices=["norm", "opt"],
        default="opt",
        help="[TM1 sources only] 'norm' for wait-based sequencing, 'opt' for explicit predecessors. "
        "Ignored for file sources (auto-detected). Default: opt",
    )
    add_log_level_arg(expand_parser)

    # --- visualize subcommand ---
    visualize_parser = subparsers.add_parser(
        "visualize",
        help="Generate interactive HTML DAG visualization",
    )
    add_taskfile_source_args(visualize_parser, required=False, include_settings=True)
    visualize_parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        required=True,
        metavar="FILE",
        help="Output HTML file path",
    )
    visualize_parser.add_argument(
        "--show-parameters",
        "-p",
        dest="show_parameters",
        action="store_true",
        default=False,
        help="Include task parameters in node labels",
    )
    visualize_parser.add_argument(
        "--mode",
        "-m",
        dest="mode",
        choices=["norm", "opt"],
        default="opt",
        help="[TM1 sources only] 'norm' for wait-based sequencing, 'opt' for explicit predecessors. "
        "Ignored for file sources (auto-detected). Default: opt",
    )
    add_log_level_arg(visualize_parser)

    # --- validate subcommand ---
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate taskfile structure and TM1 connectivity",
    )
    add_taskfile_source_args(validate_parser, required=False, include_settings=True)
    validate_parser.add_argument(
        "--skip-tm1-check",
        dest="skip_tm1_check",
        action="store_true",
        default=False,
        help="Skip TM1 connectivity and process validation",
    )
    validate_parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        default=False,
        help="Output results as JSON",
    )
    validate_parser.add_argument(
        "--mode",
        "-m",
        dest="mode",
        choices=["norm", "opt"],
        default="opt",
        help="[TM1 sources only] 'norm' for wait-based sequencing, 'opt' for explicit predecessors. "
        "Ignored for file sources (auto-detected). Default: opt",
    )
    add_log_level_arg(validate_parser)

    # Parse arguments (skip script name and 'tasks' command)
    args = parser.parse_args(argv[2:])
    apply_log_level(args.log_level)

    # Resolve config path
    config_path = resolve_config_path("config.ini", cli_path=None)

    # Dispatch to appropriate handler based on subcommand
    if args.subcommand == "export":
        handle_tasks_export(args, config_path)
    elif args.subcommand == "push":
        handle_tasks_push(args, config_path)
    elif args.subcommand == "expand":
        handle_tasks_expand(args, config_path)
    elif args.subcommand == "visualize":
        handle_tasks_visualize(args, config_path)
    elif args.subcommand == "validate":
        handle_tasks_validate(args, config_path)
