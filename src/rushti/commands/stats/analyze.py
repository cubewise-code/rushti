"""rushti stats analyze — EWMA-based historical run analysis.

Extracted from ``rushti.commands`` (formerly ``_stats_analyze``) in
Phase 2a-4 of the architecture refactor.
"""

import sys

from rushti.taskfile_ops import analyze_runs


def handle_stats_analyze(args) -> None:
    """Handle stats analyze action.

    Analyzes historical runs and generates optimization recommendations.

    :param args: Parsed arguments
    """
    try:
        from rushti.settings import load_settings
        from rushti.stats import create_stats_database, get_db_path

        settings = load_settings(args.settings_file)

        stats_db = create_stats_database(
            enabled=settings.stats.enabled,
            retention_days=settings.stats.retention_days,
            backend=settings.stats.backend,
            db_path=get_db_path(settings),
            dynamodb_region=settings.stats.dynamodb_region or None,
            dynamodb_runs_table=settings.stats.dynamodb_runs_table,
            dynamodb_task_results_table=settings.stats.dynamodb_task_results_table,
            dynamodb_endpoint_url=settings.stats.dynamodb_endpoint_url or None,
        )

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
