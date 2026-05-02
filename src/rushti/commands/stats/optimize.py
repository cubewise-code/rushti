"""rushti stats optimize — contention-aware analysis + optimized taskfile."""

import logging
import sys
from pathlib import Path

logger = logging.getLogger()


def handle_stats_optimize(args) -> None:
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
            write_contention_optimized_taskfile,
        )
        from rushti.settings import load_settings
        from rushti.stats import create_stats_database, get_db_path
        from rushti.utils import resolve_app_path

        settings = load_settings(args.settings_file)

        if not settings.stats.enabled:
            print("Error: Stats database is not enabled in settings.ini")
            print("Set [stats] enabled = true to use contention analysis")
            sys.exit(1)

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

            print(f"\nContention Analysis for: {args.workflow}")
            print("=" * 50)

            result = analyze_contention(
                stats_db=stats_db,
                workflow=args.workflow,
                sensitivity=args.sensitivity,
                lookback_runs=args.lookback_runs,
                ewma_alpha=args.ewma_alpha,
            )

            has_optimization = result.contention_driver or result.concurrency_ceiling

            if result.warnings and not has_optimization:
                print(f"\n⚠ {result.warnings[0]}")
                print("Falling back to standard optimization (longest_first).\n")

                from rushti.taskfile_ops import (
                    analyze_runs,
                    write_ewma_optimized_taskfile,
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

                write_ewma_optimized_taskfile(
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

            output_path = None
            if result.predecessor_map or result.concurrency_ceiling:
                output_path = args.output_file
                if not output_path:
                    stem = taskfile_path.stem
                    output_path = resolve_app_path(f"{stem}_optimized.json")

                write_contention_optimized_taskfile(
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

                from rushti.taskfile_ops import (
                    analyze_runs,
                    write_ewma_optimized_taskfile,
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

                write_ewma_optimized_taskfile(
                    original_taskfile_path=str(taskfile_path),
                    optimized_order=fallback_report.optimized_order,
                    output_path=output_path,
                    report=fallback_report,
                )
                print(f"\n✓ Optimized task file written to: {output_path}")
                print(f"  Use: rushti run --tasks {output_path}")

            if not args.no_report and has_optimization:
                from rushti.optimization_report import generate_optimization_report

                report_output = args.report_output
                if not report_output:
                    report_output = resolve_app_path(
                        f"visualizations/rushti_optimization_{args.workflow}.html"
                    )

                dag_path = resolve_app_path(
                    f"visualizations/rushti_optimized_dag_{args.workflow}.html"
                )

                report_filename = Path(report_output).name
                dag_filename = Path(dag_path).name

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
