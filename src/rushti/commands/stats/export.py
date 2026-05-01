"""rushti stats export — export task execution results to CSV.

Extracted from ``rushti.commands`` (formerly ``_stats_export``) in
Phase 2a-4 of the architecture refactor.
"""

import sys

from rushti.app_paths import resolve_config_path
from rushti.settings import load_settings
from rushti.stats import create_stats_database


def handle_stats_export(args) -> None:
    """Handle stats export action.

    Exports task execution results from the SQLite stats database to a CSV file.

    :param args: Parsed arguments
    """
    try:
        settings_path = resolve_config_path("settings.ini", cli_path=args.settings_file)
        settings = load_settings(settings_path)

        if not settings.stats.enabled:
            print("Error: Stats database is not enabled in settings.ini")
            print("Set [stats] enabled = true to use stats export")
            sys.exit(1)

        from rushti.stats import get_db_path
        from rushti.tm1_integration import export_results_to_csv

        stats_db = create_stats_database(
            enabled=True,
            retention_days=settings.stats.retention_days,
            backend=settings.stats.backend,
            db_path=get_db_path(settings),
            dynamodb_region=settings.stats.dynamodb_region or None,
            dynamodb_runs_table=settings.stats.dynamodb_runs_table,
            dynamodb_task_results_table=settings.stats.dynamodb_task_results_table,
            dynamodb_endpoint_url=settings.stats.dynamodb_endpoint_url or None,
        )

        try:
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
