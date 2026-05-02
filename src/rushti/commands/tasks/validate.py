"""rushti tasks validate — validate taskfile structure and TM1 connectivity."""

import os
import sys

from rushti.taskfile import TaskfileSource
from rushti.taskfile_ops import validate_taskfile_full


def handle_tasks_validate(args, config_path: str) -> None:
    """Handle tasks validate action.

    Validates a taskfile structure and optionally checks TM1 connectivity.

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

    result = validate_taskfile_full(
        source=source,
        config_path=config_path,
        check_tm1=not args.skip_tm1_check,
        output_json=args.output_json,
        mode=args.mode,
    )

    if args.output_json:
        print(result.to_json())
    else:
        result.print_human_readable()

    sys.exit(0 if result.valid else 1)
