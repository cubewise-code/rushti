"""RushTI subcommand handlers.

This module contains the handler functions for all RushTI subcommands:
- build: Create TM1 logging dimensions and cube
- resume: Resume task execution from a checkpoint
- tasks: Taskfile operations (export, push, expand, visualize, validate)
- stats: Statistics queries (export, analyze, optimize, visualize, list)
- db: Database administration (list, clear, show, vacuum)

Note: Functions from ``rushti.cli`` (argument helpers, config resolution)
are imported lazily inside each handler to avoid circular imports.
"""

import argparse
import configparser
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from TM1py import TM1Service

from rushti.settings import load_settings
from rushti.stats import create_stats_database
from rushti.taskfile import TaskfileSource
from rushti.taskfile_ops import (
    expand_taskfile,
    visualize_dag,
    validate_taskfile_full,
    analyze_runs,
)
from rushti.checkpoint import (
    load_checkpoint,
    find_checkpoint_for_taskfile,
)
from rushti.tm1_build import build_logging_objects, get_build_status

APP_NAME = "RushTI"

logger = logging.getLogger()


def run_build_command(argv: list) -> None:
    """Execute the build command to create TM1 logging objects.

    Usage: rushti build --tm1-instance tm1srv01 [--force]

    :param argv: Command line arguments
    """
    from rushti.cli import add_log_level_arg, apply_log_level, CONFIG

    parser = argparse.ArgumentParser(
        prog=f"{APP_NAME} build",
        description="Create TM1 dimensions and cube for RushTI execution logging.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  {APP_NAME} build --tm1-instance tm1srv01
  {APP_NAME} build --tm1-instance tm1srv01 --force
        """,
    )
    parser.add_argument(
        "--tm1-instance",
        dest="tm1_instance",
        required=True,
        metavar="INSTANCE",
        help="TM1 instance name from config.ini to create logging objects in",
    )
    parser.add_argument(
        "--force",
        "-f",
        dest="force",
        action="store_true",
        default=False,
        help="Delete and recreate existing objects (use with caution)",
    )
    parser.add_argument(
        "--settings",
        "-s",
        dest="settings_file",
        default=None,
        metavar="FILE",
        help="Path to settings.ini file",
    )
    add_log_level_arg(parser)

    args = parser.parse_args(argv[2:])  # Skip "rushti" and "build"
    apply_log_level(args.log_level)

    # Load settings
    settings = load_settings(args.settings_file)

    # Load config to get TM1 connection details
    if not os.path.isfile(CONFIG):
        print(f"Error: {CONFIG} does not exist")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(CONFIG, encoding="utf-8")

    if args.tm1_instance not in config.sections():
        print(f"Error: Instance '{args.tm1_instance}' not found in {CONFIG}")
        print(f"Available instances: {', '.join(config.sections())}")
        sys.exit(1)

    print(f"Connecting to TM1 instance: {args.tm1_instance}")
    try:
        params = dict(config[args.tm1_instance])
        params.pop("session_context", None)
        tm1 = TM1Service(**params)
    except Exception as e:
        print(f"Error connecting to TM1: {e}")
        sys.exit(1)

    try:
        print(f"Building RushTI logging objects (force={args.force})")
        results = build_logging_objects(
            tm1,
            force=args.force,
            cube_name=settings.tm1_integration.default_rushti_cube,
            dim_workflow=settings.tm1_integration.default_workflow_dim,
            dim_task=settings.tm1_integration.default_task_id_dim,
            dim_run=settings.tm1_integration.default_run_id_dim,
            dim_measure=settings.tm1_integration.default_measure_dim,
        )

        # Print results
        print("\nBuild Results:")
        for obj_name, created in results.items():
            status = "Created" if created else "Already exists (skipped)"
            print(f"  {obj_name}: {status}")

        # Verify all objects exist
        status = get_build_status(
            tm1,
            cube_name=settings.tm1_integration.default_rushti_cube,
            dim_workflow=settings.tm1_integration.default_workflow_dim,
            dim_task=settings.tm1_integration.default_task_id_dim,
            dim_run=settings.tm1_integration.default_run_id_dim,
            dim_measure=settings.tm1_integration.default_measure_dim,
        )
        print(f"\nStatus: {status}")

    finally:
        tm1.logout()

    sys.exit(0)


def run_resume_command(argv: list) -> Optional[dict]:
    """Run the resume subcommand - resume execution from a checkpoint.

    Usage: rushti resume [--checkpoint FILE] [--resume-from TASK_ID] [options]

    This is the ONLY way to resume from a checkpoint. The 'run' command always
    starts fresh, even if a checkpoint exists.

    :param argv: Command line arguments (sys.argv)
    """
    from rushti.cli import add_log_level_arg, apply_log_level

    parser = argparse.ArgumentParser(
        prog=f"{APP_NAME} resume",
        description="Resume task execution from a checkpoint.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  {APP_NAME} resume --checkpoint checkpoint_20250101.json
  {APP_NAME} resume --tasks tasks.json            # auto-finds checkpoint
  {APP_NAME} resume --tasks tasks.json --resume-from task-id
        """,
    )
    parser.add_argument(
        "--checkpoint",
        "-c",
        dest="checkpoint_file",
        metavar="FILE",
        default=None,
        help="Path to checkpoint file (auto-detected if not specified)",
    )
    parser.add_argument(
        "--tasks",
        "-t",
        dest="taskfile_path",
        metavar="FILE",
        default=None,
        help="Path to task file (used if not in checkpoint)",
    )
    parser.add_argument(
        "--resume-from",
        dest="resume_from",
        metavar="TASK_ID",
        default=None,
        help="Resume from specific task ID (overrides checkpoint state)",
    )
    parser.add_argument(
        "--max-workers",
        "-w",
        dest="max_workers",
        type=int,
        default=None,
        metavar="N",
        help="Maximum workers (default from settings)",
    )
    parser.add_argument(
        "--settings",
        "-s",
        dest="settings_file",
        default=None,
        metavar="FILE",
        help="Path to settings.ini",
    )
    parser.add_argument(
        "--force",
        "-f",
        dest="force",
        action="store_true",
        default=False,
        help="Force resume even if checkpoint doesn't match taskfile",
    )
    add_log_level_arg(parser)

    args = parser.parse_args(argv[2:])
    apply_log_level(args.log_level)

    # Load settings
    settings = load_settings(args.settings_file)

    # Determine checkpoint file
    checkpoint_file = args.checkpoint_file
    if not checkpoint_file:
        # Try to find checkpoint based on task file
        if args.taskfile_path:
            checkpoint_file = find_checkpoint_for_taskfile(
                settings.resume.checkpoint_dir,
                args.taskfile_path,
            )
            if not checkpoint_file:
                print(f"Error: No checkpoint found for task file: {args.taskfile_path}")
                print(f"Checkpoint directory: {settings.resume.checkpoint_dir}")
                sys.exit(1)
        else:
            # List available checkpoints
            checkpoint_dir = Path(settings.resume.checkpoint_dir)
            if checkpoint_dir.exists():
                checkpoints = list(checkpoint_dir.glob("checkpoint_*.json"))
                if checkpoints:
                    print("Available checkpoints:")
                    for cp in checkpoints:
                        try:
                            ckpt = load_checkpoint(cp)
                            print(f"  {cp.name}:")
                            print(f"    Workflow: {ckpt.workflow}")
                            print(
                                f"    Progress: {ckpt.success_count}/{ckpt.total_tasks} ({ckpt.progress_percentage:.1f}%)"
                            )
                            print(f"    Started: {ckpt.run_started}")
                        except Exception as e:
                            print(f"  {cp.name}: (error reading: {e})")
                    print(
                        "\nSpecify checkpoint with --checkpoint FILE or task file with --tasks FILE"
                    )
                else:
                    print(f"No checkpoints found in: {checkpoint_dir}")
            else:
                print(f"Checkpoint directory does not exist: {checkpoint_dir}")
            sys.exit(1)

    # Load checkpoint
    try:
        checkpoint = load_checkpoint(checkpoint_file)
    except FileNotFoundError:
        print(f"Error: Checkpoint file not found: {checkpoint_file}")
        sys.exit(1)
    except ValueError as e:
        print(f"Error: Invalid checkpoint file: {e}")
        sys.exit(1)

    # Determine task file
    taskfile_path = args.taskfile_path or checkpoint.taskfile_path

    # Validate checkpoint matches taskfile
    if os.path.exists(taskfile_path):
        is_valid, warnings = checkpoint.validate_against_taskfile(
            taskfile_path,
            strict=not args.force,
        )
        for warning in warnings:
            print(warning)
        if not is_valid and not args.force:
            print("\nUse --force to resume despite validation errors")
            sys.exit(1)
    else:
        print(f"Warning: Original task file not found: {taskfile_path}")
        if not args.force:
            print("Use --force to continue anyway")
            sys.exit(1)

    # Print resume summary (flush to ensure output appears before execution logs)
    print("\nResuming execution:", flush=True)
    print(f"  Checkpoint: {checkpoint_file}", flush=True)
    print(f"  Workflow: {checkpoint.workflow}", flush=True)
    print(f"  Started: {checkpoint.run_started}", flush=True)
    print(f"  Completed: {checkpoint.success_count}/{checkpoint.total_tasks}", flush=True)
    print(f"  Failed: {checkpoint.failure_count}", flush=True)
    print(f"  Pending: {len(checkpoint.pending_tasks)}", flush=True)
    print(f"  In-progress: {len(checkpoint.in_progress_tasks)}", flush=True)

    if args.resume_from:
        print(f"  Resuming from: {args.resume_from}", flush=True)

    print("\nStarting resumed execution...\n", flush=True)

    # Build resume context for main() to merge into cli_args
    resume_context = {
        "resume": True,
        "resume_from": args.resume_from,
        "checkpoint_file": str(checkpoint_file),
    }

    # Build argv for main execution (without resume-specific args, they're in resume_context)
    resume_argv = [
        argv[0],  # program name
        "--tasks",
        taskfile_path,
    ]

    if args.max_workers:
        resume_argv.extend(["--max-workers", str(args.max_workers)])

    if args.settings_file:
        resume_argv.extend(["--settings", args.settings_file])

    if args.force:
        resume_argv.append("--force")

    # Set sys.argv for main() to parse standard args
    sys.argv = resume_argv
    # Return the resume context for main() to merge
    return resume_context


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
    from rushti.cli import (
        add_log_level_arg,
        apply_log_level,
        add_taskfile_source_args,
        resolve_config_path,
    )

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
        _tasks_export(args, config_path)
    elif args.subcommand == "push":
        _tasks_push(args, config_path)
    elif args.subcommand == "expand":
        _tasks_expand(args, config_path)
    elif args.subcommand == "visualize":
        _tasks_visualize(args, config_path)
    elif args.subcommand == "validate":
        _tasks_validate(args, config_path)


def _tasks_export(args, config_path: str) -> None:
    """Handle tasks --export action.

    :param args: Parsed arguments
    :param config_path: Path to config.ini
    """
    # Validate output is provided
    if not args.output_file:
        print("Error: --output is required for --export")
        sys.exit(1)

    # Build TaskfileSource from arguments
    try:
        source = TaskfileSource(
            file_path=args.taskfile_path,
            tm1_instance=args.tm1_instance,
            workflow=args.workflow,
        )
        source.validate()
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Validate file exists if file source
    if source.is_file_source() and not os.path.isfile(source.file_path):
        print(f"Error: Input file not found: {source.file_path}")
        sys.exit(1)

    try:
        # Load taskfile from source
        from rushti.taskfile import load_taskfile_from_source

        taskfile = load_taskfile_from_source(source, config_path, mode=args.mode)

        # Write to JSON
        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            import json

            json.dump(taskfile.to_dict(), f, indent=2)

        print(f"Exported {len(taskfile.tasks)} tasks to: {args.output_file}")
        if source.is_tm1_source():
            print(f"  Source: TM1 {source.tm1_instance}/{source.workflow}")
        else:
            print(f"  Source: {source.file_path}")
        sys.exit(0)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def _tasks_push(args, config_path: str) -> None:
    """Handle tasks --push action.

    :param args: Parsed arguments
    :param config_path: Path to config.ini
    """
    # For push, we need a local file source
    if not args.taskfile_path:
        print("Error: --tasks is required for --push (must be a local JSON file)")
        sys.exit(1)

    if not os.path.isfile(args.taskfile_path):
        print(f"Error: Input file not found: {args.taskfile_path}")
        sys.exit(1)

    # Determine target TM1 instance
    target_instance = args.target_tm1_instance or args.tm1_instance
    if not target_instance:
        print("Error: --tm1-instance or --target-tm1-instance is required for --push")
        sys.exit(1)

    try:
        # Load and validate the taskfile
        from rushti.taskfile import parse_json_taskfile, detect_file_type

        file_type = detect_file_type(args.taskfile_path)
        if file_type != "json":
            print(f"Error: --push requires a JSON taskfile, got: {file_type}")
            sys.exit(1)

        taskfile = parse_json_taskfile(args.taskfile_path)

        # Connect to TM1 and upload
        from rushti.tm1_integration import connect_to_tm1_instance

        tm1 = connect_to_tm1_instance(target_instance, config_path)

        try:
            # Read file content
            with open(args.taskfile_path, "rb") as f:
                file_content = f.read()

            # Generate filename from workflow or input filename
            workflow = taskfile.metadata.workflow if taskfile.metadata else None
            if not workflow:
                workflow = Path(args.taskfile_path).stem

            file_name = f"rushti_taskfile_{workflow}.json"

            # Upload to TM1
            tm1.files.create(file_name=file_name, file_content=file_content)

            print(f"Pushed taskfile to TM1: {file_name}")
            print(f"  Target instance: {target_instance}")
            print(f"  Tasks: {len(taskfile.tasks)}")
            sys.exit(0)

        finally:
            try:
                tm1.logout()
            except Exception:
                pass  # Ignore logout errors; session may already be expired

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def _tasks_expand(args, config_path: str) -> None:
    """Handle tasks expand action.

    Expands MDX expressions in taskfile parameters and outputs a new taskfile
    with all parameter combinations materialized.

    :param args: Parsed arguments
    :param config_path: Path to config.ini
    """
    # Validate output is provided
    if not args.output_file:
        print("Error: --output is required for expand")
        sys.exit(1)

    # Build TaskfileSource from arguments
    try:
        source = TaskfileSource(
            file_path=args.taskfile_path,
            tm1_instance=args.tm1_instance,
            workflow=args.workflow,
        )
        source.validate()
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Validate file exists if file source
    if source.is_file_source() and not os.path.isfile(source.file_path):
        print(f"Error: Input file not found: {source.file_path}")
        sys.exit(1)

    try:
        taskfile = expand_taskfile(
            source=source,
            output_path=args.output_file,
            config_path=config_path,
            output_format=args.output_format,
            mode=args.mode,
        )
        print(f"Expanded {len(taskfile.tasks)} tasks to {args.output_file}")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def _tasks_visualize(args, config_path: str) -> None:
    """Handle tasks visualize action.

    Generates an interactive HTML DAG visualization from a taskfile.

    :param args: Parsed arguments
    :param config_path: Path to config.ini
    """
    # Build TaskfileSource from arguments
    try:
        source = TaskfileSource(
            file_path=args.taskfile_path,
            tm1_instance=args.tm1_instance,
            workflow=args.workflow,
        )
        source.validate()
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Validate file exists if file source
    if source.is_file_source() and not os.path.isfile(source.file_path):
        print(f"Error: Input file not found: {source.file_path}")
        sys.exit(1)

    try:
        result_path = visualize_dag(
            source=source,
            output_path=args.output_file,
            config_path=config_path,
            show_parameters=args.show_parameters,
            mode=args.mode,
        )
        print(f"Generated DAG visualization: {result_path}")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def _tasks_validate(args, config_path: str) -> None:
    """Handle tasks validate action.

    Validates a taskfile structure and optionally checks TM1 connectivity.

    :param args: Parsed arguments
    :param config_path: Path to config.ini
    """
    # Build TaskfileSource from arguments
    try:
        source = TaskfileSource(
            file_path=args.taskfile_path,
            tm1_instance=args.tm1_instance,
            workflow=args.workflow,
        )
        source.validate()
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Validate file exists if file source
    if source.is_file_source() and not os.path.isfile(source.file_path):
        print(f"Error: Input file not found: {source.file_path}")
        sys.exit(1)

    result = validate_taskfile_full(
        source=source,
        config_path=config_path,
        check_tm1=not args.skip_tm1_check,
        output_json=args.output_json,
        mode=args.mode,
    )

    if args.output_json:
        print(result.to_json())
    else:
        result.print_human_readable()

    sys.exit(0 if result.valid else 1)


def run_stats_command(argv: list) -> None:
    """Execute statistics and analysis commands.

    Provides tools for querying and analyzing execution data from the stats database:
    - Export results to CSV
    - Analyze historical runs for optimization
    - Optimize taskfiles using contention-aware analysis
    - Visualize execution stats as an interactive HTML dashboard
    - List runs and tasks

    Usage: rushti stats <subcommand> [options]

    :param argv: Command line arguments
    """
    from rushti.cli import add_log_level_arg, apply_log_level

    parser = argparse.ArgumentParser(
        prog=f"{APP_NAME} stats",
        description="Query and analyze execution statistics from the stats database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  {APP_NAME} stats export --workflow daily-etl --output results.csv
  {APP_NAME} stats analyze --workflow daily-etl --runs 10
  {APP_NAME} stats optimize --workflow daily-etl
  {APP_NAME} stats visualize --workflow daily-etl
  {APP_NAME} stats list runs --workflow daily-etl
  {APP_NAME} stats list tasks --workflow daily-etl
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
        help="Export execution results to CSV",
    )
    export_parser.add_argument(
        "--workflow",
        "-W",
        dest="workflow",
        required=True,
        metavar="NAME",
        help="Workflow name to export results for",
    )
    export_parser.add_argument(
        "--run-id",
        "-r",
        dest="run_id",
        default=None,
        metavar="ID",
        help="Specific run ID to export (exports all runs if not specified)",
    )
    export_parser.add_argument(
        "--output",
        "-o",
        dest="output",
        required=True,
        metavar="FILE",
        help="Output CSV file path",
    )
    export_parser.add_argument(
        "--settings",
        "-s",
        dest="settings_file",
        default=None,
        metavar="FILE",
        help="Path to settings.ini file",
    )
    add_log_level_arg(export_parser)

    # --- analyze subcommand ---
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Analyze historical runs and generate optimization recommendations",
    )
    analyze_parser.add_argument(
        "--workflow",
        "-W",
        dest="workflow",
        required=True,
        metavar="NAME",
        help="Workflow name to analyze",
    )
    analyze_parser.add_argument(
        "--tasks",
        "-t",
        dest="taskfile",
        default=None,
        metavar="FILE",
        help="Original task file (for generating optimized output)",
    )
    analyze_parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        default=None,
        metavar="FILE",
        help="Output file for optimized task file (JSON)",
    )
    analyze_parser.add_argument(
        "--report",
        dest="report_file",
        default=None,
        metavar="FILE",
        help="Output file for analysis report (JSON)",
    )
    analyze_parser.add_argument(
        "--runs",
        "-n",
        dest="run_count",
        type=int,
        default=10,
        metavar="N",
        help="Number of recent runs to analyze (default: 10)",
    )
    analyze_parser.add_argument(
        "--ewma-alpha",
        dest="ewma_alpha",
        type=float,
        default=0.3,
        metavar="ALPHA",
        help="EWMA smoothing factor 0-1 (default: 0.3, higher = more weight on recent)",
    )
    analyze_parser.add_argument(
        "--settings",
        "-s",
        dest="settings_file",
        default=None,
        metavar="FILE",
        help="Path to settings.ini file",
    )
    add_log_level_arg(analyze_parser)

    # --- optimize subcommand ---
    optimize_parser = subparsers.add_parser(
        "optimize",
        help="Contention-aware optimization: detect resource contention and generate optimized taskfile",
    )
    optimize_parser.add_argument(
        "--workflow",
        "-W",
        dest="workflow",
        required=True,
        metavar="NAME",
        help="Workflow name to analyze for contention patterns",
    )
    optimize_parser.add_argument(
        "--tasks",
        "-t",
        dest="taskfile",
        default=None,
        metavar="FILE",
        help="Input task file (JSON). If not provided, uses the archived taskfile from the most recent run",
    )
    optimize_parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        default=None,
        metavar="FILE",
        help="Output path for optimized task file (default: <taskfile>_optimized.json)",
    )
    optimize_parser.add_argument(
        "--sensitivity",
        dest="sensitivity",
        type=float,
        default=10.0,
        metavar="K",
        help="IQR multiplier for outlier detection (default: 10.0, higher = more conservative)",
    )
    optimize_parser.add_argument(
        "--runs",
        "-n",
        dest="lookback_runs",
        type=int,
        default=10,
        metavar="N",
        help="Number of recent runs for EWMA estimation (default: 10)",
    )
    optimize_parser.add_argument(
        "--ewma-alpha",
        dest="ewma_alpha",
        type=float,
        default=0.3,
        metavar="ALPHA",
        help="EWMA smoothing factor 0-1 (default: 0.3, higher = more weight on recent)",
    )
    optimize_parser.add_argument(
        "--settings",
        "-s",
        dest="settings_file",
        default=None,
        metavar="FILE",
        help="Path to settings.ini file",
    )
    optimize_parser.add_argument(
        "--no-report",
        dest="no_report",
        action="store_true",
        default=False,
        help="Skip generating the HTML optimization report",
    )
    optimize_parser.add_argument(
        "--report-output",
        dest="report_output",
        default=None,
        metavar="FILE",
        help="Output path for HTML optimization report (default: alongside taskfile)",
    )
    add_log_level_arg(optimize_parser)

    # --- visualize subcommand ---
    visualize_parser = subparsers.add_parser(
        "visualize",
        help="Generate an interactive HTML dashboard for a taskfile",
    )
    visualize_parser.add_argument(
        "--workflow",
        "-W",
        dest="workflow",
        required=True,
        metavar="NAME",
        help="Workflow name to visualize",
    )
    visualize_parser.add_argument(
        "--runs",
        "-n",
        dest="run_count",
        type=int,
        default=5,
        metavar="N",
        help="Number of recent runs to display initially (default: 5)",
    )
    visualize_parser.add_argument(
        "--output",
        "-o",
        dest="output",
        default=None,
        metavar="FILE",
        help="Output HTML file path (default: visualizations/rushti_dashboard_<workflow>.html)",
    )
    visualize_parser.add_argument(
        "--settings",
        "-s",
        dest="settings_file",
        default=None,
        metavar="FILE",
        help="Path to settings.ini file",
    )
    add_log_level_arg(visualize_parser)

    # --- list subcommand ---
    list_parser = subparsers.add_parser(
        "list",
        help="List runs or tasks for a taskfile",
    )
    list_parser.add_argument(
        "list_type",
        choices=["runs", "tasks"],
        help="What to list: runs or tasks",
    )
    list_parser.add_argument(
        "--workflow",
        "-W",
        dest="workflow",
        required=True,
        metavar="NAME",
        help="Workflow name to list data for",
    )
    list_parser.add_argument(
        "--limit",
        "-n",
        dest="limit",
        type=int,
        default=20,
        metavar="N",
        help="Maximum number of items to show (default: 20)",
    )
    list_parser.add_argument(
        "--settings",
        "-s",
        dest="settings_file",
        default=None,
        metavar="FILE",
        help="Path to settings.ini file",
    )
    add_log_level_arg(list_parser)

    # Parse arguments
    args = parser.parse_args(argv[2:])
    apply_log_level(args.log_level)

    # Dispatch to appropriate handler
    if args.subcommand == "export":
        _stats_export(args)
    elif args.subcommand == "analyze":
        _stats_analyze(args)
    elif args.subcommand == "optimize":
        _stats_optimize(args)
    elif args.subcommand == "visualize":
        _stats_visualize(args)
    elif args.subcommand == "list":
        _stats_list(args)


def _stats_export(args) -> None:
    """Handle stats export action.

    Exports task execution results from the SQLite stats database to a CSV file.

    :param args: Parsed arguments
    """
    from rushti.cli import resolve_config_path

    try:
        # Load settings
        settings_path = resolve_config_path("settings.ini", cli_path=args.settings_file)
        settings = load_settings(settings_path)

        # Check if stats are enabled
        if not settings.stats.enabled:
            print("Error: Stats database is not enabled in settings.ini")
            print("Set [stats] enabled = true to use stats export")
            sys.exit(1)

        # Import here to avoid circular imports
        from rushti.tm1_integration import export_results_to_csv

        # Create stats database connection
        stats_db = create_stats_database(
            enabled=True,
            retention_days=settings.stats.retention_days,
        )

        try:
            # Export results
            row_count = export_results_to_csv(
                stats_db=stats_db,
                workflow=args.workflow,
                run_id=args.run_id,
                output_path=args.output,
            )

            if row_count > 0:
                print(f"Exported {row_count} task results to: {args.output}")
                if args.run_id:
                    print(f"  Run ID: {args.run_id}")
                else:
                    print(f"  Workflow: {args.workflow} (all runs)")
                sys.exit(0)
            else:
                print(f"No results found for workflow '{args.workflow}'")
                if args.run_id:
                    print(f"  Run ID: {args.run_id}")
                sys.exit(1)
        finally:
            stats_db.close()

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def _stats_analyze(args) -> None:
    """Handle stats analyze action.

    Analyzes historical runs and generates optimization recommendations.

    :param args: Parsed arguments
    """
    try:
        # Load settings
        from rushti.settings import load_settings
        from rushti.stats import StatsDatabase, get_db_path

        settings = load_settings(args.settings_file)

        # Initialize stats database
        stats_db = StatsDatabase(db_path=get_db_path(settings), enabled=settings.stats.enabled)

        # Run analysis
        report = analyze_runs(
            workflow=args.workflow,
            stats_db=stats_db,
            output_path=args.report_file,
            run_count=args.run_count,
            ewma_alpha=args.ewma_alpha,
        )

        print(f"\nAnalysis Report for: {args.workflow}")
        print("=" * 40)
        print(f"Runs analyzed: {report.run_count}")
        print(f"Tasks analyzed: {len(report.tasks)}")
        print(f"  - With history: {len([t for t in report.tasks if not t.estimated])}")
        print(f"  - Estimated (no history): {len([t for t in report.tasks if t.estimated])}")

        # Show confidence statistics
        tasks_with_history = [t for t in report.tasks if not t.estimated]
        if tasks_with_history:
            avg_confidence = sum(t.confidence for t in tasks_with_history) / len(tasks_with_history)
            high_conf = len([t for t in tasks_with_history if t.confidence >= 0.8])
            print("\nConfidence scores:")
            print(f"  - Average: {avg_confidence:.2f}")
            print(f"  - High confidence (≥0.8): {high_conf}/{len(tasks_with_history)}")

        if report.recommendations:
            print("\nRecommendations:")
            for rec in report.recommendations:
                print(f"  {rec}")

        # Write optimized task file if requested
        if args.output_file:
            if not args.taskfile:
                print("\nWarning: --tasks required to generate optimized task file")
                print("Only analysis report will be written.")
            else:
                from rushti.taskfile_ops import write_optimized_taskfile

                write_optimized_taskfile(
                    original_taskfile_path=args.taskfile,
                    optimized_order=report.optimized_order,
                    output_path=args.output_file,
                    report=report,
                )
                print(f"\n✓ Optimized task file written to: {args.output_file}")

        if args.report_file:
            print(f"✓ Analysis report written to: {args.report_file}")

        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def _stats_optimize(args) -> None:
    """Handle stats optimize action.

    Runs contention-aware analysis on a workflow and generates an optimized
    taskfile with predecessor chains that prevent heavy tasks from running
    concurrently.

    :param args: Parsed arguments
    """
    try:
        from rushti.contention_analyzer import (
            analyze_contention,
            get_archived_taskfile_path,
            write_optimized_taskfile,
        )
        from rushti.settings import load_settings
        from rushti.stats import StatsDatabase, get_db_path
        from rushti.utils import resolve_app_path

        settings = load_settings(args.settings_file)

        if not settings.stats.enabled:
            print("Error: Stats database is not enabled in settings.ini")
            print("Set [stats] enabled = true to use contention analysis")
            sys.exit(1)

        # Initialize stats database
        stats_db = StatsDatabase(db_path=get_db_path(settings), enabled=True)

        try:
            # Resolve taskfile: explicit --tasks flag, or auto-resolve from archive
            if args.taskfile:
                taskfile_path = Path(args.taskfile)
                if not taskfile_path.exists():
                    print(f"Error: Task file not found: {args.taskfile}")
                    sys.exit(1)
            else:
                archived_path = get_archived_taskfile_path(stats_db, args.workflow)
                if not archived_path:
                    print(
                        f"Error: No archived taskfile found for workflow '{args.workflow}'.\n"
                        "Run the workflow at least once, or provide --tasks explicitly."
                    )
                    sys.exit(1)
                taskfile_path = Path(archived_path)
                if not taskfile_path.exists():
                    print(
                        f"Error: Archived taskfile no longer exists: {archived_path}\n"
                        "Provide --tasks explicitly."
                    )
                    sys.exit(1)
                print(f"Using archived taskfile: {taskfile_path}")

            # Run contention analysis
            print(f"\nContention Analysis for: {args.workflow}")
            print("=" * 50)

            result = analyze_contention(
                stats_db=stats_db,
                workflow=args.workflow,
                sensitivity=args.sensitivity,
                lookback_runs=args.lookback_runs,
                ewma_alpha=args.ewma_alpha,
            )

            # Display results
            has_optimization = result.contention_driver or result.concurrency_ceiling

            if result.warnings and not has_optimization:
                print(f"\n⚠ {result.warnings[0]}")
                print("Falling back to standard optimization (longest_first).\n")

                # Actually perform the fallback: use analyze_runs for EWMA
                # longest_first reordering, then write an optimized taskfile.
                from rushti.taskfile_ops import analyze_runs
                from rushti.taskfile_ops import (
                    write_optimized_taskfile as write_optimized_taskfile_standard,
                )

                report = analyze_runs(
                    workflow=args.workflow,
                    stats_db=stats_db,
                    run_count=args.lookback_runs,
                    ewma_alpha=args.ewma_alpha,
                )

                fallback_output = args.output_file
                if not fallback_output:
                    fallback_output = resolve_app_path(f"{taskfile_path.stem}_optimized.json")

                write_optimized_taskfile_standard(
                    original_taskfile_path=str(taskfile_path),
                    optimized_order=report.optimized_order,
                    output_path=fallback_output,
                    report=report,
                )
                print(f"✓ Optimized task file written to: {fallback_output}")
                print(f"  Use: rushti run --tasks {fallback_output}")
                print(
                    "\n⚠ Note: Contention-aware optimization is based on statistical "
                    "analysis of historical\n  execution data. The recommendations "
                    "follow theoretical reasoning but actual TM1\n  server behavior "
                    "depends on factors beyond task ordering — server load, memory,\n"
                    "  concurrent users, and data volumes. Test the optimized taskfile "
                    "before deploying\n  to production."
                )
                sys.exit(0)

            # Display contention driver analysis (if found)
            if result.contention_driver:
                print(f"\nContention driver: {result.contention_driver}")
                print(f"Fan-out parameters: {', '.join(result.fan_out_keys) or 'none'}")
                print(f"Fan-out size: {result.fan_out_size}")
                print(f"Total tasks: {result.total_tasks}")

                print(f"\nIQR statistics (sensitivity k={result.sensitivity}):")
                print(f"  Q1: {result.iqr_stats.get('q1', 0):.1f}s")
                print(f"  Q3: {result.iqr_stats.get('q3', 0):.1f}s")
                print(f"  IQR: {result.iqr_stats.get('iqr', 0):.1f}s")
                print(f"  Upper fence: {result.iqr_stats.get('upper_fence', 0):.1f}s")

                print(f"\nHeavy groups ({len(result.heavy_groups)}):")
                for g in result.heavy_groups:
                    print(f"  {g.driver_value}: {g.avg_duration:.1f}s ({len(g.task_ids)} tasks)")
                if not result.heavy_groups:
                    print("  (none detected)")

                print(f"\nLight groups ({len(result.light_groups)}):")
                for g in result.light_groups[:5]:
                    print(f"  {g.driver_value}: {g.avg_duration:.1f}s ({len(g.task_ids)} tasks)")
                if len(result.light_groups) > 5:
                    print(f"  ... and {len(result.light_groups) - 5} more")

                if result.predecessor_map:
                    print("\nChain structure:")
                    print(f"  Chain length: {result.chain_length} heavy groups")
                    print(f"  Chains: {result.fan_out_size} independent chains")
                    print(f"  Tasks with predecessors: {len(result.predecessor_map)}")
                    print(f"  Critical path: {result.critical_path_seconds:.1f}s")
                else:
                    print("\nNo predecessor chains generated.")

            # Display concurrency ceiling / scale-up analysis (if found)
            if result.concurrency_ceiling:
                evidence = result.ceiling_evidence or {}
                confidence = evidence.get("confidence", "")
                if confidence == "scale_up":
                    print(
                        f"\nScale-up opportunity detected: recommend increasing to "
                        f"{result.concurrency_ceiling} workers"
                    )
                    best = evidence.get("best_level", {})
                    worst = evidence.get("worst_level", {})
                    print(
                        f"  {worst.get('max_workers')} workers: "
                        f"{worst.get('wall_clock', 0):.0f}s wall clock (slowest)"
                    )
                    print(
                        f"  {best.get('max_workers')} workers: "
                        f"{best.get('wall_clock', 0):.0f}s wall clock (fastest)"
                    )
                    print(
                        f"  Potential improvement: "
                        f"{evidence.get('wall_clock_improvement', 0):.0f}s "
                        f"({evidence.get('wall_clock_improvement_pct', 0):.1f}%)"
                    )
                else:
                    print(
                        f"\nConcurrency ceiling detected: " f"{result.concurrency_ceiling} workers"
                    )
                    if confidence == "multi_run":
                        best = evidence.get("best_level", {})
                        worst = evidence.get("worst_level", {})
                        print(
                            f"  {worst.get('max_workers')} workers: "
                            f"{worst.get('wall_clock', 0):.0f}s wall clock"
                        )
                        print(
                            f"  {best.get('max_workers')} workers: "
                            f"{best.get('wall_clock', 0):.0f}s wall clock"
                        )
                        print(
                            f"  Improvement: "
                            f"{evidence.get('wall_clock_improvement', 0):.0f}s "
                            f"({evidence.get('wall_clock_improvement_pct', 0):.1f}%)"
                        )
                    elif confidence == "single_run":
                        print(
                            f"  Correlation (concurrency↔duration): "
                            f"{evidence.get('correlation', 0):.2f}"
                        )
                        print(
                            f"  Effective parallelism: "
                            f"{evidence.get('effective_parallelism', 0):.1f}"
                            f"/{evidence.get('max_workers_used', 0)}"
                        )
                        print(f"  Efficiency: {evidence.get('efficiency', 0):.1%}")

            print(f"\nRecommended max_workers: {result.recommended_workers}")

            if result.warnings:
                print("\nWarnings:")
                for w in result.warnings:
                    print(f"  ⚠ {w}")

            # Write optimized taskfile
            output_path = None
            if result.predecessor_map or result.concurrency_ceiling:
                output_path = args.output_file
                if not output_path:
                    stem = taskfile_path.stem
                    output_path = resolve_app_path(f"{stem}_optimized.json")

                write_optimized_taskfile(
                    original_taskfile_path=str(taskfile_path),
                    result=result,
                    output_path=output_path,
                )
                print(f"\n✓ Optimized task file written to: {output_path}")
                print(f"  Use: rushti run --tasks {output_path}")
            else:
                # Driver found but insufficient heavy groups for chains
                # and no concurrency ceiling — fall back to longest_first
                print(
                    "\nNo contention chains generated — falling back to "
                    "standard optimization (longest_first)."
                )

                from rushti.taskfile_ops import analyze_runs
                from rushti.taskfile_ops import (
                    write_optimized_taskfile as write_optimized_taskfile_standard,
                )

                fallback_report = analyze_runs(
                    workflow=args.workflow,
                    stats_db=stats_db,
                    run_count=args.lookback_runs,
                    ewma_alpha=args.ewma_alpha,
                )

                output_path = args.output_file
                if not output_path:
                    output_path = resolve_app_path(f"{taskfile_path.stem}_optimized.json")

                write_optimized_taskfile_standard(
                    original_taskfile_path=str(taskfile_path),
                    optimized_order=fallback_report.optimized_order,
                    output_path=output_path,
                    report=fallback_report,
                )
                print(f"\n✓ Optimized task file written to: {output_path}")
                print(f"  Use: rushti run --tasks {output_path}")

            # Generate DAG visualization + HTML optimization report
            if not args.no_report and has_optimization:
                from rushti.optimization_report import generate_optimization_report

                # HTML outputs go under visualizations/ (same pattern as _stats_visualize)
                report_output = args.report_output
                if not report_output:
                    report_output = resolve_app_path(
                        f"visualizations/rushti_optimization_{args.workflow}.html"
                    )

                dag_path = resolve_app_path(
                    f"visualizations/rushti_optimized_dag_{args.workflow}.html"
                )

                # Cross-link filenames (both in same visualizations/ dir)
                report_filename = Path(report_output).name
                dag_filename = Path(dag_path).name

                # Generate DAG from optimized taskfile (if output was written)
                dag_generated = False
                if output_path and result.predecessor_map:
                    try:
                        from rushti.taskfile_ops import visualize_dag

                        Path(dag_path).parent.mkdir(parents=True, exist_ok=True)
                        visualize_dag(
                            source=output_path,
                            output_path=dag_path,
                            dashboard_url=report_filename,
                        )
                        dag_generated = True
                        print(f"  DAG visualization: {dag_path}")
                    except Exception as e:
                        logger.warning(f"Could not generate DAG visualization: {e}")
                        print(f"  Warning: DAG visualization skipped ({e})")

                generate_optimization_report(
                    workflow=args.workflow,
                    result=result,
                    output_path=report_output,
                    dag_url=dag_filename if dag_generated else None,
                )
                print(f"  Optimization report: {report_output}")

            print(
                "\n⚠ Note: Contention-aware optimization is based on statistical "
                "analysis of historical\n  execution data. The recommendations "
                "follow theoretical reasoning but actual TM1\n  server behavior "
                "depends on factors beyond task ordering — server load, memory,\n"
                "  concurrent users, and data volumes. Test the optimized taskfile "
                "before deploying\n  to production."
            )

        finally:
            stats_db.close()

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def _stats_visualize(args) -> None:
    """Handle stats visualize action.

    Generates an interactive HTML dashboard AND DAG visualization for a
    taskfile and opens the dashboard in the default browser.

    :param args: Parsed arguments
    """
    import webbrowser
    from pathlib import Path

    from rushti.dashboard import generate_dashboard
    from rushti.db_admin import get_visualization_data
    from rushti.settings import load_settings
    from rushti.stats import get_db_path
    from rushti.utils import resolve_app_path

    settings = load_settings(getattr(args, "settings_file", None))
    db_path = get_db_path(settings)

    try:
        print(f"Generating visualizations for workflow: {args.workflow}")

        data = get_visualization_data(args.workflow, db_path)
        if not data.get("exists"):
            print(f"Error: {data.get('message')}")
            sys.exit(1)

        # Determine output paths
        if args.output:
            dashboard_path = args.output
            # Derive DAG path from the same directory
            dashboard_p = Path(args.output)
            dag_path = str(dashboard_p.parent / f"rushti_dag_{args.workflow}.html")
        else:
            dashboard_path = resolve_app_path(
                f"visualizations/rushti_dashboard_{args.workflow}.html"
            )
            dag_path = resolve_app_path(f"visualizations/rushti_dag_{args.workflow}.html")

        # Relative filenames for cross-links (both files in same folder)
        dashboard_filename = Path(dashboard_path).name
        dag_filename = Path(dag_path).name

        # --- Attempt DAG generation ---
        dag_generated = False
        taskfile_path = None

        # Find the first accessible taskfile_path from runs (most recent first)
        runs = data["runs"]
        for run in runs:
            candidate = run.get("taskfile_path")
            if candidate and not candidate.startswith("TM1:") and os.path.isfile(candidate):
                taskfile_path = candidate
                break

        if taskfile_path:
            try:
                from rushti.taskfile_ops import visualize_dag

                # Ensure output directory exists
                Path(dag_path).parent.mkdir(parents=True, exist_ok=True)

                visualize_dag(
                    source=taskfile_path,
                    output_path=dag_path,
                    dashboard_url=dashboard_filename,
                )
                dag_generated = True
                print(f"DAG visualization generated: {dag_path}")
            except Exception as e:
                logger.warning(f"Could not generate DAG visualization: {e}")
                print(f"Warning: DAG visualization skipped ({e})")
        else:
            print(
                "Warning: No accessible taskfile found in run history, skipping DAG visualization"
            )

        # --- Generate dashboard ---
        output_file = generate_dashboard(
            workflow=args.workflow,
            runs=data["runs"],
            task_results=data["task_results"],
            output_path=dashboard_path,
            default_runs=args.run_count,
            dag_url=dag_filename if dag_generated else None,
        )

        print(f"Dashboard generated: {output_file}")

        # Try to open in browser (non-blocking, errors are not fatal)
        try:
            abs_path = os.path.abspath(output_file)
            webbrowser.open(f"file://{abs_path}")
        except Exception:
            pass

        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def _stats_list(args) -> None:
    """Handle stats list action.

    Lists runs or tasks for a taskfile.

    :param args: Parsed arguments
    """
    from rushti.db_admin import list_runs, list_tasks
    from rushti.settings import load_settings
    from rushti.stats import get_db_path

    settings = load_settings(getattr(args, "settings_file", None))
    db_path = get_db_path(settings)

    try:
        if args.list_type == "runs":
            runs = list_runs(args.workflow, db_path, limit=args.limit)
            if not runs:
                print(f"No runs found for workflow: {args.workflow}")
                sys.exit(0)

            print(f"\nRuns for {args.workflow} (showing {len(runs)})")
            print("=" * 100)
            print(
                f"{'Run ID':<25} {'Start Time':<20} {'Tasks':<8} {'Success':<10} {'Duration(s)':<12}"
            )
            print("-" * 100)
            for run in runs:
                print(
                    f"{run['run_id']:<25} {run['start_time']:<20} "
                    f"{run['task_count']:<8} {run['success_rate']:>6.1f}%   "
                    f"{run['total_duration']:>10.2f}"
                )

        elif args.list_type == "tasks":
            tasks = list_tasks(args.workflow, db_path)
            if not tasks:
                print(f"No tasks found for workflow: {args.workflow}")
                sys.exit(0)

            # Apply limit manually if needed
            if args.limit and len(tasks) > args.limit:
                tasks = tasks[: args.limit]
                showing_text = f"showing {len(tasks)} of {len(tasks)}"
            else:
                showing_text = f"showing {len(tasks)}"

            print(f"\nTasks for {args.workflow} ({showing_text})")
            print("=" * 100)
            print(
                f"{'Task Signature':<50} {'Executions':<12} {'Success Rate':<15} {'Avg Duration(s)'}"
            )
            print("-" * 100)
            for task in tasks:
                print(
                    f"{task['task_signature']:<50} {task['execution_count']:<12} "
                    f"{task['success_rate']:>6.1f}%        {task['avg_duration']:>10.2f}"
                )

        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def run_db_command(argv: list) -> None:
    """Execute database administration commands.

    Provides administrative tools for managing the SQLite stats database:
    - List all taskfiles
    - Clear/delete data
    - Show run and task details
    - Maintenance operations (vacuum)

    Note: For querying statistics, exporting data, and analyzing runs,
    use the 'stats' command instead.

    Usage: rushti db <subcommand> [options]

    :param argv: Command line arguments
    """
    import json

    from rushti.db_admin import (
        list_workflows,
        clear_workflow,
        clear_run,
        clear_before_date,
        clear_all,
        vacuum_database,
        show_run_details,
        show_task_history,
    )
    from rushti.settings import load_settings
    from rushti.stats import get_db_path

    # Check for help flag or no subcommand
    if len(argv) < 3 or (len(argv) == 3 and argv[2] in ("--help", "-h")):
        print(f"""
{APP_NAME} db - Database Administration

Usage:
  {APP_NAME} db <subcommand> [options]

Subcommands:
  list workflows            List all workflows in database

  clear --workflow NAME     Delete all data for a workflow
  clear --run-id ID         Delete data for a specific run
  clear --before DATE       Delete data before date (YYYY-MM-DD)
  clear --all               Delete all data (requires confirmation)

  show run --run-id ID      Show details for a specific run
  show task --signature SIG Show execution history for a task

  vacuum                    Optimize database size

Options:
  --dry-run                Preview changes without executing (for clear commands)
  --settings FILE          Path to settings.ini

Examples:
  {APP_NAME} db list workflows
  {APP_NAME} db clear --workflow old_workflow --dry-run
  {APP_NAME} db show run --run-id 20250102_120000
  {APP_NAME} db vacuum

Note: For statistics, exports, and analysis, use '{APP_NAME} stats' command.
        """)
        sys.exit(0)

    subcommand = argv[2]

    # For list/show commands, we need to handle an extra positional arg
    list_type = None
    show_type = None

    # Determine if there's a sub-subcommand (e.g., "list taskfiles", "show run")
    if subcommand in ["list", "show"] and len(argv) > 3 and not argv[3].startswith("-"):
        if subcommand == "list":
            list_type = argv[3]
            args_start = 4
        elif subcommand == "show":
            show_type = argv[3]
            args_start = 4
    else:
        args_start = 3

    # Parse remaining arguments
    parser = argparse.ArgumentParser(prog=f"{APP_NAME} db {subcommand}", add_help=False)
    parser.add_argument("--workflow", "-W", dest="workflow")
    parser.add_argument("--run-id", "-r", dest="run_id")
    parser.add_argument("--signature", dest="signature")
    parser.add_argument("--before", dest="before_date")
    parser.add_argument("--output", "-o", dest="output")
    parser.add_argument("--all", dest="all_data", action="store_true")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("--settings", "-s", dest="settings_file")
    parser.add_argument("--limit", "-n", dest="limit", type=int, default=20)

    args = parser.parse_args(argv[args_start:])

    # Get database path from settings
    settings = load_settings(args.settings_file)
    db_path = get_db_path(settings)

    try:
        if subcommand == "list":
            if not list_type:
                print("Error: Specify what to list: workflows")
                print(f"For listing runs or tasks, use '{APP_NAME} stats list'")
                sys.exit(1)

            if list_type == "workflows":
                workflows = list_workflows(db_path)
                if not workflows:
                    print("No workflows found in database")
                    sys.exit(0)

                print(f"\nWorkflows ({len(workflows)})")
                print("=" * 80)
                print(f"{'Workflow':<40} {'Runs':<8} {'Tasks':<8} {'Last Run':<20}")
                print("-" * 80)
                for wf in workflows:
                    print(
                        f"{wf['workflow']:<40} {wf['run_count']:<8} "
                        f"{wf['task_count']:<8} {wf['last_run']:<20}"
                    )
            else:
                print(f"Error: '{list_type}' is not available in 'db list'")
                print(f"Use '{APP_NAME} stats list {list_type}' instead")
                sys.exit(1)

        elif subcommand == "clear":
            # Determine what to clear
            if args.workflow:
                count = clear_workflow(args.workflow, db_path, dry_run=args.dry_run)
                if args.dry_run:
                    print(f"Would delete {count} records for workflow: {args.workflow}")
                else:
                    if count == 0:
                        print(f"No records found for workflow: {args.workflow}")
                    else:
                        print(f"✓ Deleted {count} records for workflow: {args.workflow}")

            elif args.run_id:
                count = clear_run(args.run_id, db_path, dry_run=args.dry_run)
                if args.dry_run:
                    print(f"Would delete {count} records for run: {args.run_id}")
                else:
                    if count == 0:
                        print(f"No records found for run: {args.run_id}")
                    else:
                        print(f"✓ Deleted {count} records for run: {args.run_id}")

            elif args.before_date:
                count = clear_before_date(args.before_date, db_path, dry_run=args.dry_run)
                if args.dry_run:
                    print(f"Would delete {count} records before {args.before_date}")
                else:
                    if count == 0:
                        print(f"No records found before {args.before_date}")
                    else:
                        print(f"✓ Deleted {count} records before {args.before_date}")

            elif args.all_data:
                if not args.dry_run:
                    # Require confirmation for --all
                    response = input(
                        "Are you sure you want to delete ALL database records? (yes/no): "
                    )
                    if response.lower() != "yes":
                        print("Cancelled")
                        sys.exit(0)

                count = clear_all(db_path, dry_run=args.dry_run)
                if args.dry_run:
                    print(f"Would delete all {count} records from database")
                else:
                    print(f"✓ Deleted all {count} records from database")

            else:
                print("Error: Specify what to clear: --workflow, --run-id, --before, or --all")
                sys.exit(1)

        elif subcommand == "show":
            if not show_type:
                print("Error: Specify what to show: run or task")
                sys.exit(1)

            if show_type == "run":
                if not args.run_id:
                    print("Error: --run-id required")
                    sys.exit(1)

                details = show_run_details(args.run_id, db_path)
                if not details.get("exists"):
                    print(f"Error: {details.get('message')}")
                    sys.exit(1)

                print(f"\nRun Details: {details['run_id']}")
                print("=" * 80)
                print(f"Workflow:           {details['workflow']}")
                print(f"Start time:         {details['start_time']}")
                print(f"End time:           {details['end_time']}")
                print(f"Total duration:     {details['total_duration']}s")
                print(f"Task count:         {details['task_count']}")
                print(f"Success count:      {details['success_count']}")
                print(f"Error count:        {details['error_count']}")
                print(f"Success rate:       {details['success_rate']}%")

                print(f"\nTasks ({len(details['tasks'])}):")
                print("-" * 80)
                for task in details["tasks"][:20]:
                    status_icon = "✓" if task["status"] == "Success" else "✗"
                    print(
                        f"  {status_icon} {task['task_id']:<15} {task['duration']:>8.2f}s  {task['status']}"
                    )
                    if task["error"]:
                        print(f"     Error: {task['error'][:60]}...")
                if len(details["tasks"]) > 20:
                    print(f"\n... and {len(details['tasks']) - 20} more tasks")

            elif show_type == "task":
                if not args.signature:
                    print("Error: --signature required")
                    sys.exit(1)

                history = show_task_history(args.signature, db_path, limit=args.limit)
                if not history.get("exists"):
                    print(f"Error: {history.get('message')}")
                    sys.exit(1)

                print("\nTask History")
                print("=" * 80)
                print(f"Task ID:            {history['task_id']}")
                print(f"Signature:          {history['task_signature']}")
                print(f"Instance:           {history['instance']}")
                print(f"Process:            {history['process']}")
                print(f"Parameters:         {json.dumps(history['parameters'])}")
                print(f"Total executions:   {history['execution_count']}")

                print(f"\nRecent Executions (showing {len(history['executions'])}):")
                print("-" * 80)
                print(f"{'Run ID':<25} {'Start Time':<20} {'Duration':<12} {'Status':<20}")
                print("-" * 80)
                for exec in history["executions"]:
                    print(
                        f"{exec['run_id']:<25} {exec['start_time']:<20} "
                        f"{exec['duration']:>8.2f}s    {exec['status']:<20}"
                    )

            else:
                print(f"Error: Unknown show type '{show_type}'. Use: run or task")
                sys.exit(1)

        elif subcommand == "vacuum":
            size_before, size_after = vacuum_database(db_path)
            saved_mb = (size_before - size_after) / 1024 / 1024

            print("\nDatabase Optimized")
            print("=" * 60)
            print(f"Size before:        {size_before / 1024 / 1024:.2f} MB")
            print(f"Size after:         {size_after / 1024 / 1024:.2f} MB")
            print(f"Space saved:        {saved_mb:.2f} MB")

        else:
            print(f"Error: Unknown subcommand '{subcommand}'")
            print(f"Run '{APP_NAME} db' for help")
            sys.exit(1)

        sys.exit(0)

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
