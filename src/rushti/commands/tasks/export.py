"""rushti tasks export — convert TXT/TM1 taskfiles to JSON."""

import json
import os
import sys
from pathlib import Path

from rushti.taskfile import TaskfileSource


def handle_tasks_export(args, config_path: str) -> None:
    """Handle tasks export action.

    :param args: Parsed arguments
    :param config_path: Path to config.ini
    """
    if not args.output_file:
        print("Error: --output is required for --export")
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
        from rushti.taskfile import load_taskfile_from_source

        taskfile = load_taskfile_from_source(source, config_path, mode=args.mode)

        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(taskfile.to_dict(), f, indent=2)

        print(f"Exported {len(taskfile.tasks)} tasks to: {args.output_file}")
        if source.is_tm1_source():
            print(f"  Source: TM1 {source.tm1_instance}/{source.workflow}")
        else:
            print(f"  Source: {source.file_path}")
        sys.exit(0)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
