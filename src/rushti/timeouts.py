"""Run phase time limits for RushTI runs (issue #124, ADR 0004).

A run goes through four run phases, each with an optional time limit
(``[timeouts]`` in settings.ini, ``settings.timeouts`` in the taskfile,
``--timeout-<phase>`` on the CLI). A phase going past its limit is a hang.
Only elapsed time counts; TM1 server state is never polled.

On a hang the main thread is stuck inside a blocking TM1 call, so the
watchdog runs on its own thread and ends the process with ``os._exit``
(``sys.exit`` from a non-main thread doesn't exit the process).
"""

import argparse
import logging
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

from rushti.execution import ExecutionContext, logout_in_background
from rushti.results_writer import create_results_file
from rushti.settings import RUN_PHASES
from rushti.stats.sqlite import StatsDatabase

if TYPE_CHECKING:
    from rushti.checkpoint import CheckpointManager

logger = logging.getLogger(__name__)

#: Longest the watchdog waits, in total, for the TM1 logouts on a hang
LOGOUT_WAIT_SECONDS = 5

PHASE_HELP = {
    "task_retrieval": "connecting to the source instance and reading the taskfile from the cube",
    "connection": (
        "connecting to the task instances until the first task starts "
        "(the exclusive-mode wait doesn't count)"
    ),
    "task_execution": "from the first task starting to the last one finishing",
    "push_results": "pushing results to the results target and loading them into the cube",
}


def _seconds(value: str) -> int:
    try:
        seconds = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"must be a whole number of seconds, got '{value}'")
    if seconds < 0:
        raise argparse.ArgumentTypeError(f"must be non-negative, got {seconds}")
    return seconds


def _flag(phase: str) -> str:
    return "--timeout-" + phase.replace("_", "-")


def add_timeout_args(parser: argparse.ArgumentParser) -> None:
    """Add the ``--timeout-<phase>`` arguments to a parser."""
    for phase in RUN_PHASES:
        parser.add_argument(
            _flag(phase),
            dest=f"timeout_{phase}",
            type=_seconds,
            default=None,
            metavar="SECONDS",
            help=(
                f"Time limit for the {phase} run phase: {PHASE_HELP[phase]}. "
                "0 = no limit. Default: from settings.ini [timeouts], or no limit"
            ),
        )


def timeout_cli_args(args: argparse.Namespace) -> Dict[str, Optional[int]]:
    """CLI time limits as ``cli_args`` entries (``timeout_<phase>``)."""
    return {f"timeout_{phase}": getattr(args, f"timeout_{phase}") for phase in RUN_PHASES}


def timeout_argv(args: argparse.Namespace) -> List[str]:
    """The ``--timeout-<phase>`` arguments that were given, ready to forward."""
    argv = []
    for phase in RUN_PHASES:
        seconds = getattr(args, f"timeout_{phase}")
        if seconds is not None:  # 0 is a real value: it removes a lower-tier limit
            argv.extend([_flag(phase), str(seconds)])
    return argv


class PhaseWatchdog:
    """Times the current run phase and calls ``on_hang(phase, limit)`` if it runs over.

    One phase is timed at a time: starting a phase ends the previous one.
    The timer thread is a daemon, so it never keeps the process alive.
    """

    def __init__(self, on_hang: Callable[[str, int], None]):
        self._on_hang = on_hang
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None
        self._generation = 0  # tells a timer that fired late it's no longer current
        self._phase: Optional[str] = None
        self._limit = 0
        self._deadline = 0.0
        self._remaining: Optional[float] = None  # seconds left while paused

    def start(self, phase: str, limit: int) -> None:
        """Start timing ``phase``, ending whichever phase was running. 0 means no limit."""
        with self._lock:
            self._cancel()
            self._phase, self._limit, self._remaining = phase, limit, None
            if limit:
                self._arm(limit)

    def stop(self) -> None:
        """End the current phase."""
        with self._lock:
            self._cancel()
            self._phase, self._remaining = None, None

    @contextmanager
    def phase(self, phase: str, limit: int):
        """Time the block as ``phase``."""
        self.start(phase, limit)
        try:
            yield
        finally:
            self.stop()

    @contextmanager
    def paused(self):
        """Stop the clock of the current phase while the block runs."""
        with self._lock:
            if self._timer:
                self._remaining = max(0.0, self._deadline - time.monotonic())
                self._cancel()
        try:
            yield
        finally:
            with self._lock:
                # start() or stop() inside the block clears _remaining
                if self._remaining is not None:
                    self._arm(self._remaining)
                    self._remaining = None

    def _arm(self, seconds: float) -> None:
        self._generation += 1
        self._deadline = time.monotonic() + seconds
        self._timer = threading.Timer(seconds, self._fire, args=(self._generation,))
        self._timer.daemon = True
        self._timer.start()

    def _cancel(self) -> None:
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def _fire(self, generation: int) -> None:
        with self._lock:
            if self._timer is None or generation != self._generation:
                return  # the phase ended or was paused just as the limit hit
            self._timer = None
            phase, limit = self._phase, self._limit
        self._on_hang(phase, limit)


@dataclass
class HangHandler:
    """Ends a hung run: what the watchdog does when a run phase goes past its limit.

    ``cli.main()`` keeps the attributes current as the run goes on, so the
    handler acts on whatever the run had set up when the limit was hit.
    Nothing here may call TM1 except the logouts, which run in the background.
    """

    start_time: datetime
    result_file: str = ""
    ctx: Optional[ExecutionContext] = None
    #: Set while the stats run is open; cleared once main() has written it
    stats_db: Optional[object] = None
    #: Set while the checkpoint is live; cleared once main() has cleaned it up
    checkpoint_manager: Optional["CheckpointManager"] = None
    #: Task instance sessions; preserved connections are skipped unless forced
    tm1_services: dict = field(default_factory=dict)
    tm1_preserve_connections: dict = field(default_factory=dict)
    force_logout: bool = False
    #: Source and results target sessions, which are never preserved
    other_sessions: dict = field(default_factory=dict)

    def __call__(self, phase: str, limit: int) -> None:
        logger.error(
            f"Run phase '{phase}' went past its time limit of {limit}s. "
            "RushTI is terminating and logging out of TM1."
        )
        for step in (
            self._write_result,
            self._save_checkpoint,
            self._flush_execution_logs,
            self._write_stats,
            self._logout,
        ):
            try:
                step()
            except Exception as e:
                logger.error(f"Cleanup after the hang failed at {step.__name__}: {e}")
        logging.shutdown()
        os._exit(1)

    def _outcomes(self) -> List[bool]:
        return list(self.ctx.task_outcomes) if self.ctx else []

    def _write_result(self) -> None:
        if not self.result_file:
            return
        outcomes = self._outcomes()
        end_time = datetime.now()
        create_results_file(
            self.result_file,
            False,
            len(outcomes),
            len(outcomes) - sum(outcomes),
            self.start_time,
            end_time,
            end_time - self.start_time,
            status="TimedOut",
        )

    def _save_checkpoint(self) -> None:
        # Save, don't clean up: the checkpoint is what 'rushti resume' starts from
        if self.checkpoint_manager:
            self.checkpoint_manager.force_save()
            logger.info(
                f"Checkpoint retained for resume: {self.checkpoint_manager.checkpoint_path}"
            )

    def _flush_execution_logs(self) -> None:
        if self.ctx and self.ctx.execution_logger and self.ctx.execution_logger.log_count > 0:
            self.ctx.execution_logger.flush()

    def _write_stats(self) -> None:
        if self.stats_db is None:
            return
        if not isinstance(self.stats_db, StatsDatabase):
            logger.warning("Stats not written: only the local SQLite backend is written on a hang")
            return
        with self.ctx.stats_data_lock:
            rows = list(self.ctx.stats_data)
            self.ctx.stats_data.clear()
        if rows:
            self.stats_db.batch_record_tasks(rows)
        outcomes = self._outcomes()
        successes = sum(outcomes)
        self.stats_db.complete_run(
            run_id=self.ctx.execution_logger.run_id,
            status="Partial" if successes else "Failed",
            success_count=successes,
            failure_count=len(outcomes) - successes,
        )

    def _logout(self) -> None:
        sessions = [
            (name, service)
            for name, service in list(self.tm1_services.items())
            if self.force_logout or self.tm1_preserve_connections.get(name) is not True
        ]
        sessions += list(self.other_sessions.items())
        for name in logout_in_background(sessions, LOGOUT_WAIT_SECONDS):
            logger.warning(
                f"Logout from {name} didn't finish within {LOGOUT_WAIT_SECONDS}s; giving up on it"
            )
