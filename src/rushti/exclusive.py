"""Exclusive mode module for RushTI.

This module provides exclusive execution mode functionality to ensure
only one RushTI instance runs at a time across all clients in shared
TM1 environments.

Session identification uses TM1 session context fields:
- Normal mode: RushTI{workflow}
- Exclusive mode: RushTIX{workflow}
"""

import logging
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from TM1py import TM1Service

# Use root logger for consistent logging with cli.py
logger = logging.getLogger()

# Context field format patterns
RUSHTI_CONTEXT_PREFIX = "RushTI"
RUSHTI_EXCLUSIVE_PREFIX = "RushTIX"
CONTEXT_PATTERN = re.compile(r"^RushTI(X?)(.*)$")

# TM1 context field maximum length (64 characters)
TM1_CONTEXT_MAX_LENGTH = 64


@dataclass
class RushTISession:
    """Represents an active RushTI session detected on a TM1 server."""

    instance_name: str
    workflow: str
    is_exclusive: bool
    thread_id: int
    user_name: Optional[str] = None

    def __str__(self) -> str:
        mode = "exclusive" if self.is_exclusive else "normal"
        return f"RushTI session '{self.workflow}' ({mode}) on {self.instance_name}"


def build_session_context(workflow: str = "", exclusive: bool = False) -> str:
    """Build the TM1 session context string for RushTI.

    :param workflow: The workflow name (may be empty)
    :param exclusive: Whether this is an exclusive mode session
    :return: Context string (e.g., "RushTI_daily-etl" or "RushTIX_daily-etl",
             or "RushTI" / "RushTIX" when workflow is empty)
    """
    prefix = RUSHTI_EXCLUSIVE_PREFIX if exclusive else RUSHTI_CONTEXT_PREFIX
    context = f"{prefix}_{workflow}" if workflow else prefix

    # Truncate if exceeds TM1 limit
    if len(context) > TM1_CONTEXT_MAX_LENGTH:
        logger.warning(
            f"Session context '{context}' exceeds {TM1_CONTEXT_MAX_LENGTH} chars, "
            f"truncating to fit TM1 limit"
        )
        context = context[:TM1_CONTEXT_MAX_LENGTH]

    return context


def parse_session_context(context: str) -> Optional[Tuple[bool, str]]:
    """Parse a TM1 session context string to extract RushTI info.

    :param context: The session context string
    :return: Tuple of (is_exclusive, workflow) or None if not a RushTI session
    """
    if not context:
        return None

    match = CONTEXT_PATTERN.match(context)
    if not match:
        return None

    is_exclusive = match.group(1) == "X"
    workflow = match.group(2)
    return is_exclusive, workflow


def check_active_rushti_sessions(
    tm1_services: Dict[str, TM1Service],
    exclude_context: Optional[str] = None,
) -> List[RushTISession]:
    """Check all TM1 instances for active RushTI sessions.

    Uses MonitoringService.get_sessions(include_threads=True) to find all sessions
    and their threads, then filters by RushTI context.

    :param tm1_services: Dictionary of TM1Service instances by server name
    :param exclude_context: Context string to exclude (our own session context)
    :return: List of detected RushTI sessions
    """
    active_sessions = []

    if exclude_context:
        logger.debug(f"Excluding own session context: {exclude_context}")

    for instance_name, tm1 in tm1_services.items():
        try:
            # Get our own session to exclude it
            current_session = tm1.monitoring.get_current_user()
            current_session_id = None
            if isinstance(current_session, dict):
                current_session_id = current_session.get("ID")
            elif hasattr(current_session, "ID"):
                current_session_id = current_session.ID

            logger.debug(f"Current session ID on {instance_name}: {current_session_id}")

            # Get ALL sessions with their threads
            all_sessions = tm1.monitoring.get_sessions(include_threads=True)
            logger.debug(f"Found {len(all_sessions)} sessions on {instance_name}")

            for session in all_sessions:
                # Get session ID
                session_id = None
                if isinstance(session, dict):
                    session_id = session.get("ID")
                elif hasattr(session, "ID"):
                    session_id = session.ID

                # Skip our own session
                if current_session_id and session_id == current_session_id:
                    logger.debug(f"Skipping own session ID: {session_id}")
                    continue

                # Get user name from session
                user_name = None
                if isinstance(session, dict):
                    user_info = session.get("User")
                    if isinstance(user_info, dict):
                        user_name = user_info.get("Name")
                    elif user_info:
                        user_name = str(user_info)

                # Get threads for this session
                threads = []
                if isinstance(session, dict):
                    threads = session.get("Threads") or []
                elif hasattr(session, "Threads"):
                    threads = session.Threads or []

                for thread in threads:
                    # Get context from thread
                    context = None
                    if isinstance(thread, dict):
                        context = thread.get("Context")
                    elif hasattr(thread, "Context"):
                        context = thread.Context

                    if not context:
                        continue

                    # Skip if matches our exclude context
                    if exclude_context and context == exclude_context:
                        logger.debug(f"Skipping excluded context: {context}")
                        continue

                    parsed = parse_session_context(context)
                    if parsed:
                        is_exclusive, workflow = parsed

                        # Get thread ID
                        thread_id = None
                        if isinstance(thread, dict):
                            thread_id = thread.get("ID")
                        elif hasattr(thread, "ID"):
                            thread_id = thread.ID

                        session_obj = RushTISession(
                            instance_name=instance_name,
                            workflow=workflow,
                            is_exclusive=is_exclusive,
                            thread_id=thread_id,
                            user_name=user_name,
                        )
                        active_sessions.append(session_obj)
                        logger.debug(f"Detected RushTI session: {session_obj}")

        except Exception as e:
            logger.warning(f"Failed to check sessions on {instance_name}: {e}")

    return active_sessions


def should_wait_for_sessions(
    active_sessions: List[RushTISession],
    current_exclusive: bool,
) -> Tuple[bool, List[RushTISession]]:
    """Determine if execution should wait based on active sessions.

    Wait logic:
    - If current run is exclusive AND any RushTI session is active: wait
    - If current run is non-exclusive AND any exclusive session is active: wait
    - If current run is non-exclusive AND only non-exclusive running: proceed

    :param active_sessions: List of detected RushTI sessions
    :param current_exclusive: Whether current run requests exclusive mode
    :return: Tuple of (should_wait, blocking_sessions)
    """
    if not active_sessions:
        return False, []

    if current_exclusive:
        # Exclusive mode: wait for ANY RushTI session
        return True, active_sessions

    # Non-exclusive mode: only wait for exclusive sessions
    exclusive_sessions = [s for s in active_sessions if s.is_exclusive]
    if exclusive_sessions:
        return True, exclusive_sessions

    return False, []


def wait_for_exclusive_access(
    tm1_services: Dict[str, TM1Service],
    current_exclusive: bool,
    session_context: Optional[str] = None,
    polling_interval: int = 30,
    timeout: int = 600,
    force: bool = False,
) -> bool:
    """Wait for exclusive access to TM1 servers.

    Polls for active RushTI sessions and waits until they complete
    or timeout is exceeded.

    :param tm1_services: Dictionary of TM1Service instances
    :param current_exclusive: Whether current run requests exclusive mode
    :param session_context: Our own session context string to exclude from checks
    :param polling_interval: Seconds between session checks (default: 30)
    :param timeout: Maximum seconds to wait (default: 600)
    :param force: If True, bypass exclusive checks with warning
    :return: True if access granted, raises ExclusiveModeTimeoutError on timeout
    :raises ExclusiveModeTimeoutError: If timeout exceeded while waiting
    """
    mode_desc = "exclusive" if current_exclusive else "non-exclusive"
    logger.debug(f"Checking for active RushTI sessions ({mode_desc} mode)")

    # Check for active sessions (excluding our own)
    active_sessions = check_active_rushti_sessions(tm1_services, exclude_context=session_context)
    should_wait, blocking_sessions = should_wait_for_sessions(active_sessions, current_exclusive)

    if not should_wait:
        logger.info(f"No blocking RushTI sessions detected, proceeding with {mode_desc} execution")
        return True

    # Handle force flag
    if force:
        session_info = ", ".join(str(s) for s in blocking_sessions)
        logger.warning(
            f"Force flag used - bypassing exclusive mode checks. "
            f"Active sessions: {session_info}"
        )
        return True

    # Log initial blocking status before entering wait loop
    session_info = "; ".join(str(s) for s in blocking_sessions)
    logger.info(
        f"Exclusive access required ({mode_desc} mode). "
        f"Found {len(blocking_sessions)} blocking session(s): {session_info}"
    )
    logger.info(
        f"Will retry every {polling_interval}s until access granted or timeout ({timeout}s)"
    )

    # Wait loop
    elapsed = 0
    attempt = 0

    while elapsed < timeout:
        attempt += 1
        time.sleep(polling_interval)
        elapsed += polling_interval

        # Re-check sessions (excluding our own)
        logger.debug(f"Checking for active sessions (attempt {attempt}, elapsed: {elapsed}s)")
        active_sessions = check_active_rushti_sessions(
            tm1_services, exclude_context=session_context
        )
        should_wait, blocking_sessions = should_wait_for_sessions(
            active_sessions, current_exclusive
        )

        if not should_wait:
            logger.info(f"Exclusive access granted after waiting {elapsed}s ({attempt} attempt(s))")
            return True

        # Log progress at each interval
        session_info = "; ".join(str(s) for s in blocking_sessions)
        logger.info(
            f"Still waiting for exclusive access "
            f"(attempt {attempt}, elapsed: {elapsed}s/{timeout}s). "
            f"Blocking: {session_info}"
        )

    # Timeout exceeded
    session_info = "; ".join(str(s) for s in blocking_sessions)
    raise ExclusiveModeTimeoutError(
        f"Timeout ({timeout}s) exceeded waiting for exclusive access. "
        f"Blocking sessions: {session_info}"
    )


class ExclusiveModeTimeoutError(Exception):
    """Raised when exclusive mode wait times out."""

    pass
