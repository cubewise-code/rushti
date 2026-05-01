"""rushti build — create TM1 logging dimensions and cube.

Usage: ``rushti build --tm1-instance tm1srv01 [--force]``

Extracted from ``rushti.commands`` in Phase 2 of the architecture
refactor (see ``docs/architecture/refactoring-plan.md``).
"""

import argparse
import configparser
import os
import sys

from TM1py import TM1Service

from rushti.logging_setup import add_log_level_arg, apply_log_level
from rushti.settings import load_settings
from rushti.tm1_build import build_logging_objects, get_build_status
from rushti.tm1_integration import resolve_tm1_params

APP_NAME = "RushTI"


def run_build_command(argv: list) -> None:
    """Execute the build command to create TM1 logging objects.

    Usage: rushti build --tm1-instance tm1srv01 [--force]

    :param argv: Command line arguments
    """
    from rushti.cli import CONFIG

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
        params = resolve_tm1_params(config, args.tm1_instance)
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
