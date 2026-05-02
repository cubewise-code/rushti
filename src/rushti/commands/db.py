"""rushti db — database administration commands.

Administrative tools for managing the SQLite stats database (list,
clear, show, vacuum). For querying statistics and analyzing runs, see
the ``stats`` command instead.
"""

import argparse
import json
import sys
import traceback

from rushti.db_admin import (
    clear_all,
    clear_before_date,
    clear_run,
    clear_workflow,
    list_workflows,
    show_run_details,
    show_task_history,
    vacuum_database,
)
from rushti.settings import load_settings
from rushti.stats import get_db_path, get_stats_backend

APP_NAME = "RushTI"


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
    if get_stats_backend(settings) != "sqlite":
        print("Error: 'db' command currently supports only [stats] backend = sqlite")
        sys.exit(1)
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
                        f"  {status_icon} {task['task_id']:<15} "
                        f"{task['duration']:>8.2f}s  {task['status']}"
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
        traceback.print_exc()
        sys.exit(1)
