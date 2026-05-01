"""rushti stats — statistics queries dispatcher.

Subcommands:
    export:    Export task execution results to CSV
    analyze:   Analyze historical runs (EWMA-based optimization)
    optimize:  Contention-aware analysis + optimized taskfile
    visualize: Generate interactive HTML dashboard + DAG
    list:      List runs or tasks for a workflow

Extracted from ``rushti.commands`` in Phase 2a-4 of the architecture
refactor (see ``docs/architecture/refactoring-plan.md``).
"""

import argparse

from rushti.commands.stats.analyze import handle_stats_analyze
from rushti.commands.stats.export import handle_stats_export
from rushti.commands.stats.list import handle_stats_list
from rushti.commands.stats.optimize import handle_stats_optimize
from rushti.commands.stats.visualize import handle_stats_visualize
from rushti.logging_setup import add_log_level_arg, apply_log_level

APP_NAME = "RushTI"


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
        handle_stats_export(args)
    elif args.subcommand == "analyze":
        handle_stats_analyze(args)
    elif args.subcommand == "optimize":
        handle_stats_optimize(args)
    elif args.subcommand == "visualize":
        handle_stats_visualize(args)
    elif args.subcommand == "list":
        handle_stats_list(args)
