"""rushti stats list — list runs or tasks for a workflow.

Extracted from ``rushti.commands`` (formerly ``_stats_list``) in
Phase 2a-4 of the architecture refactor.
"""

import sys


def handle_stats_list(args) -> None:
    """Handle stats list action.

    Lists runs or tasks for a taskfile.

    :param args: Parsed arguments
    """
    from rushti.db_admin import list_runs, list_tasks
    from rushti.settings import load_settings
    from rushti.stats import create_stats_database, get_db_path, get_stats_backend

    settings = load_settings(getattr(args, "settings_file", None))
    backend = get_stats_backend(settings)
    db_path = get_db_path(settings)

    stats_db = None
    try:
        stats_db = create_stats_database(
            enabled=True,
            backend=backend,
            db_path=db_path,
            dynamodb_region=settings.stats.dynamodb_region or None,
            dynamodb_runs_table=settings.stats.dynamodb_runs_table,
            dynamodb_task_results_table=settings.stats.dynamodb_task_results_table,
            dynamodb_endpoint_url=settings.stats.dynamodb_endpoint_url or None,
        )

        if args.list_type == "runs":
            runs = list_runs(args.workflow, stats_db, limit=args.limit)
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
            tasks = list_tasks(args.workflow, stats_db)
            if not tasks:
                print(f"No tasks found for workflow: {args.workflow}")
                sys.exit(0)

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
    finally:
        if stats_db is not None:
            stats_db.close()
