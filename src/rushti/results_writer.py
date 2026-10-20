"""CSV results-summary writer for RushTI runs.

Writes a small two-row CSV summarizing a run's outcome (PID, executions,
fails, start/end times, duration, overall success, status). The path is supplied
by the caller (typically from the ``--result`` CLI arg or the
``[defaults] result_file`` setting).
"""

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

from rushti.utils import ensure_shared_file, makedirs_shared

__all__ = ["create_results_file"]


def create_results_file(
    result_file: str,
    overall_success: bool,
    executions: int,
    fails: int,
    start_time: datetime,
    end_time: datetime,
    elapsed_time: timedelta,
    status: str,
) -> None:
    """Write a one-record CSV summary of the run to ``result_file``.

    ``status`` is the run outcome: ``Success`` (every task succeeded),
    ``Failed`` (at least one task failed), ``Aborted`` (stopped on an error)
    or ``TimedOut`` (a run phase went past its time limit). It's the last
    column, so readers of the older columns are unaffected.

    Creates the parent directory if needed. The CSV uses ``|`` as the
    field separator (matching legacy convention) and ensures the
    resulting file is writable by all OS users (multi-user installs).
    """
    header = (
        "PID",
        "Process Runs",
        "Process Fails",
        "Start",
        "End",
        "Runtime",
        "Overall Success",
        "Status",
    )
    record = (
        os.getpid(),
        executions,
        fails,
        start_time,
        end_time,
        elapsed_time,
        overall_success,
        status,
    )

    makedirs_shared(str(Path(result_file).parent))
    with open(result_file, "w", encoding="utf-8") as file:
        cw = csv.writer(file, delimiter="|", lineterminator="\n")
        cw.writerows([header, record])
    ensure_shared_file(result_file)
