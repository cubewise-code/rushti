"""rushti stats visualize — interactive HTML dashboard + DAG.

Extracted from ``rushti.commands`` (formerly ``_stats_visualize``) in
Phase 2a-4 of the architecture refactor.
"""

import logging
import os
import sys

logger = logging.getLogger()


def handle_stats_visualize(args) -> None:
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
    from rushti.stats import create_stats_database, get_db_path, get_stats_backend
    from rushti.utils import resolve_app_path

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
        data = get_visualization_data(
            args.workflow,
            stats_db,
            include_all_workflows=True,
        )

        print(f"Generating visualizations for workflow: {args.workflow}")
        if not data.get("exists"):
            print(f"Error: {data.get('message')}")
            sys.exit(1)

        # Determine output paths
        if args.output:
            dashboard_path = args.output
            dashboard_p = Path(args.output)
            dag_path = str(dashboard_p.parent / f"rushti_dag_{args.workflow}.html")
        else:
            dashboard_path = resolve_app_path(
                f"visualizations/rushti_dashboard_{args.workflow}.html"
            )
            dag_path = resolve_app_path(f"visualizations/rushti_dag_{args.workflow}.html")

        dashboard_filename = Path(dashboard_path).name
        dag_filename = Path(dag_path).name

        # --- Attempt DAG generation ---
        dag_generated = False

        # Prefer DB-based DAG (no taskfile on disk needed); fall back to taskfile if unavailable.
        runs = data["runs"]
        workflow_lower = args.workflow.lower()
        workflow_runs = [r for r in runs if (r.get("workflow") or "").lower() == workflow_lower]
        latest_run = workflow_runs[0] if workflow_runs else None

        dag_generated_from_db = False
        if latest_run:
            try:
                from rushti.taskfile_ops import visualize_dag_from_db_results

                latest_run_id = latest_run["run_id"]
                latest_task_results = [
                    tr for tr in data["task_results"] if tr["run_id"] == latest_run_id
                ]

                if latest_task_results:
                    Path(dag_path).parent.mkdir(parents=True, exist_ok=True)
                    visualize_dag_from_db_results(
                        task_results=latest_task_results,
                        output_path=dag_path,
                        dashboard_url=dashboard_filename,
                    )
                    dag_generated = True
                    dag_generated_from_db = True
                    print(f"DAG visualization generated: {dag_path}")
            except Exception as e:
                logger.warning(f"Could not generate DAG from DB: {e}")

        if not dag_generated_from_db:
            # Fall back to taskfile on disk (most recent accessible one for this workflow)
            taskfile_path = None
            for run in workflow_runs:
                candidate = run.get("taskfile_path")
                if candidate and not candidate.startswith("TM1:") and os.path.isfile(candidate):
                    taskfile_path = candidate
                    break

            if taskfile_path:
                try:
                    from rushti.taskfile_ops import visualize_dag

                    Path(dag_path).parent.mkdir(parents=True, exist_ok=True)
                    visualize_dag(
                        source=taskfile_path,
                        output_path=dag_path,
                        dashboard_url=dashboard_filename,
                    )
                    dag_generated = True
                    print(f"DAG visualization generated from taskfile: {dag_path}")
                except Exception as e:
                    logger.warning(f"Could not generate DAG visualization: {e}")
                    print(f"Warning: DAG visualization skipped ({e})")
            else:
                print(
                    "Warning: No DB task results or accessible taskfile found, skipping DAG visualization"
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
    finally:
        if stats_db is not None:
            stats_db.close()
