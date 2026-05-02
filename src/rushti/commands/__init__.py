"""RushTI subcommand handlers (package facade).

Each subcommand lives in its own focused module beneath this package:

- ``commands.build``  — ``run_build_command``: create TM1 logging objects
- ``commands.resume`` — ``run_resume_command``: resume from checkpoint
- ``commands.tasks``  — ``run_tasks_command``: taskfile operations
                        (export, push, expand, visualize, validate)
- ``commands.stats``  — ``run_stats_command``: statistics queries
                        (export, analyze, optimize, visualize, list)
- ``commands.db``     — ``run_db_command``: database administration
                        (list, clear, show, vacuum)

This ``__init__`` re-exports the entry-point handlers so callers can
keep using ``from rushti.commands import run_build_command`` etc.
without caring about the internal package layout.
"""

# Backwards-compatible re-exports so callers can keep using
# ``from rushti.commands import run_build_command`` etc.
from rushti.commands.build import run_build_command
from rushti.commands.db import run_db_command
from rushti.commands.resume import run_resume_command
from rushti.commands.stats import run_stats_command
from rushti.commands.tasks import run_tasks_command

__all__ = [
    "run_build_command",
    "run_db_command",
    "run_resume_command",
    "run_stats_command",
    "run_tasks_command",
]
