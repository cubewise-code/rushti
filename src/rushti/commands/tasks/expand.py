"""rushti tasks expand — expand MDX expressions in taskfile parameters.

Extracted from ``rushti.commands`` (formerly ``_tasks_expand``) in
Phase 2a-3 of the architecture refactor.
"""

import os
import sys

from rushti.taskfile import TaskfileSource
from rushti.taskfile_ops import expand_taskfile


def handle_tasks_expand(args, config_path: str) -> None:
    """Handle tasks expand action.

    Expands MDX expressions in taskfile parameters and outputs a new taskfile
    with all parameter combinations materialized.

    :param args: Parsed arguments
    :param config_path: Path to config.ini
    """
    if not args.output_file:
        print("Error: --output is required for expand")
        sys.exit(1)

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
