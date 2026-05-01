"""rushti tasks visualize — generate interactive HTML DAG visualization.

Extracted from ``rushti.commands`` (formerly ``_tasks_visualize``) in
Phase 2a-3 of the architecture refactor.
"""

import os
import sys

from rushti.taskfile import TaskfileSource
from rushti.taskfile_ops import visualize_dag


def handle_tasks_visualize(args, config_path: str) -> None:
    """Handle tasks visualize action.

    Generates an interactive HTML DAG visualization from a taskfile.

    :param args: Parsed arguments
    :param config_path: Path to config.ini
    """
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
