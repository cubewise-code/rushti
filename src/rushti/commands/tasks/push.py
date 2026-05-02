"""rushti tasks push — upload JSON taskfile to TM1 as a file."""

import os
import sys
from pathlib import Path


def handle_tasks_push(args, config_path: str) -> None:
    """Handle tasks push action.

    :param args: Parsed arguments
    :param config_path: Path to config.ini
    """
    if not args.taskfile_path:
        print("Error: --tasks is required for --push (must be a local JSON file)")
        sys.exit(1)

    if not os.path.isfile(args.taskfile_path):
        print(f"Error: Input file not found: {args.taskfile_path}")
        sys.exit(1)

    target_instance = args.target_tm1_instance or args.tm1_instance
    if not target_instance:
        print("Error: --tm1-instance or --target-tm1-instance is required for --push")
        sys.exit(1)

    try:
        from rushti.taskfile import detect_file_type, parse_json_taskfile

        file_type = detect_file_type(args.taskfile_path)
        if file_type != "json":
            print(f"Error: --push requires a JSON taskfile, got: {file_type}")
            sys.exit(1)

        taskfile = parse_json_taskfile(args.taskfile_path)

        from rushti.tm1_integration import connect_to_tm1_instance

        tm1 = connect_to_tm1_instance(target_instance, config_path)

        try:
            with open(args.taskfile_path, "rb") as f:
                file_content = f.read()

            workflow = taskfile.metadata.workflow if taskfile.metadata else None
            if not workflow:
                workflow = Path(args.taskfile_path).stem

            file_name = f"rushti_taskfile_{workflow}.json"

            tm1.files.create(file_name=file_name, file_content=file_content)

            print(f"Pushed taskfile to TM1: {file_name}")
            print(f"  Target instance: {target_instance}")
            print(f"  Tasks: {len(taskfile.tasks)}")
            sys.exit(0)

        finally:
            try:
                tm1.logout()
            except Exception:
                pass  # Ignore logout errors; session may already be expired

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
