"""rushti resume — resume task execution from a checkpoint.

This is the ONLY way to resume from a checkpoint. The 'run' command
always starts fresh, even if a checkpoint exists.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

from rushti.checkpoint import find_checkpoint_for_taskfile, load_checkpoint
from rushti.logging_setup import add_log_level_arg, apply_log_level
from rushti.settings import load_settings

APP_NAME = "RushTI"


def run_resume_command(argv: list) -> Optional[dict]:
    """Run the resume subcommand - resume execution from a checkpoint.

    Usage: rushti resume [--checkpoint FILE] [--resume-from TASK_ID] [options]

    This is the ONLY way to resume from a checkpoint. The 'run' command always
    starts fresh, even if a checkpoint exists.

    :param argv: Command line arguments (sys.argv)
    """
    parser = argparse.ArgumentParser(
        prog=f"{APP_NAME} resume",
        description="Resume task execution from a checkpoint.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  {APP_NAME} resume --checkpoint checkpoint_20250101.json
  {APP_NAME} resume --tasks tasks.json            # auto-finds checkpoint
  {APP_NAME} resume --tasks tasks.json --resume-from task-id
        """,
    )
    parser.add_argument(
        "--checkpoint",
        "-c",
        dest="checkpoint_file",
        metavar="FILE",
        default=None,
        help="Path to checkpoint file (auto-detected if not specified)",
    )
    parser.add_argument(
        "--tasks",
        "-t",
        dest="taskfile_path",
        metavar="FILE",
        default=None,
        help="Path to task file (used if not in checkpoint)",
    )
    parser.add_argument(
        "--resume-from",
        dest="resume_from",
        metavar="TASK_ID",
        default=None,
        help="Resume from specific task ID (overrides checkpoint state)",
    )
    parser.add_argument(
        "--max-workers",
        "-w",
        dest="max_workers",
        type=int,
        default=None,
        metavar="N",
        help="Maximum workers (default from settings)",
    )
    parser.add_argument(
        "--settings",
        "-s",
        dest="settings_file",
        default=None,
        metavar="FILE",
        help="Path to settings.ini",
    )
    parser.add_argument(
        "--force",
        "-f",
        dest="force",
        action="store_true",
        default=False,
        help="Force resume even if checkpoint doesn't match taskfile",
    )
    add_log_level_arg(parser)

    args = parser.parse_args(argv[2:])
    apply_log_level(args.log_level)

    # Load settings
    settings = load_settings(args.settings_file)

    # Determine checkpoint file
    checkpoint_file = args.checkpoint_file
    if not checkpoint_file:
        # Try to find checkpoint based on task file
        if args.taskfile_path:
            checkpoint_file = find_checkpoint_for_taskfile(
                settings.resume.checkpoint_dir,
                args.taskfile_path,
            )
            if not checkpoint_file:
                print(f"Error: No checkpoint found for task file: {args.taskfile_path}")
                print(f"Checkpoint directory: {settings.resume.checkpoint_dir}")
                sys.exit(1)
        else:
            # List available checkpoints
            checkpoint_dir = Path(settings.resume.checkpoint_dir)
            if checkpoint_dir.exists():
                checkpoints = list(checkpoint_dir.glob("checkpoint_*.json"))
                if checkpoints:
                    print("Available checkpoints:")
                    for cp in checkpoints:
                        try:
                            ckpt = load_checkpoint(cp)
                            print(f"  {cp.name}:")
                            print(f"    Workflow: {ckpt.workflow}")
                            print(
                                f"    Progress: {ckpt.success_count}/{ckpt.total_tasks} "
                                f"({ckpt.progress_percentage:.1f}%)"
                            )
                            print(f"    Started: {ckpt.run_started}")
                        except Exception as e:
                            print(f"  {cp.name}: (error reading: {e})")
                    print(
                        "\nSpecify checkpoint with --checkpoint FILE or task file with --tasks FILE"
                    )
                else:
                    print(f"No checkpoints found in: {checkpoint_dir}")
            else:
                print(f"Checkpoint directory does not exist: {checkpoint_dir}")
            sys.exit(1)

    # Load checkpoint
    try:
        checkpoint = load_checkpoint(checkpoint_file)
    except FileNotFoundError:
        print(f"Error: Checkpoint file not found: {checkpoint_file}")
        sys.exit(1)
    except ValueError as e:
        print(f"Error: Invalid checkpoint file: {e}")
        sys.exit(1)

    # Determine task file
    taskfile_path = args.taskfile_path or checkpoint.taskfile_path

    # Validate checkpoint matches taskfile
    if os.path.exists(taskfile_path):
        is_valid, warnings = checkpoint.validate_against_taskfile(
            taskfile_path,
            strict=not args.force,
        )
        for warning in warnings:
            print(warning)
        if not is_valid and not args.force:
            print("\nUse --force to resume despite validation errors")
            sys.exit(1)
    else:
        print(f"Warning: Original task file not found: {taskfile_path}")
        if not args.force:
            print("Use --force to continue anyway")
            sys.exit(1)

    # Print resume summary (flush to ensure output appears before execution logs)
    print("\nResuming execution:", flush=True)
    print(f"  Checkpoint: {checkpoint_file}", flush=True)
    print(f"  Workflow: {checkpoint.workflow}", flush=True)
    print(f"  Started: {checkpoint.run_started}", flush=True)
    print(f"  Completed: {checkpoint.success_count}/{checkpoint.total_tasks}", flush=True)
    print(f"  Failed: {checkpoint.failure_count}", flush=True)
    print(f"  Pending: {len(checkpoint.pending_tasks)}", flush=True)
    print(f"  In-progress: {len(checkpoint.in_progress_tasks)}", flush=True)

    if args.resume_from:
        print(f"  Resuming from: {args.resume_from}", flush=True)

    print("\nStarting resumed execution...\n", flush=True)

    # Build resume context for main() to merge into cli_args
    resume_context = {
        "resume": True,
        "resume_from": args.resume_from,
        "checkpoint_file": str(checkpoint_file),
    }

    # Build argv for main execution (without resume-specific args, they're in resume_context)
    resume_argv = [
        argv[0],  # program name
        "--tasks",
        taskfile_path,
    ]

    if args.max_workers:
        resume_argv.extend(["--max-workers", str(args.max_workers)])

    if args.settings_file:
        resume_argv.extend(["--settings", args.settings_file])

    if args.force:
        resume_argv.append("--force")

    # Set sys.argv for main() to parse standard args
    sys.argv = resume_argv
    # Return the resume context for main() to merge
    return resume_context
