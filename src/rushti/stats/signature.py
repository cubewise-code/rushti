"""Deterministic task-signature hashing."""

import hashlib
import json
from typing import Any, Dict, Optional


def calculate_task_signature(
    instance: str,
    process: Optional[str],
    parameters: Optional[Dict[str, Any]],
    chore: Optional[str] = None,
) -> str:
    """Calculate deterministic signature for task identity.

    The signature uniquely identifies a task configuration for
    runtime estimation across multiple runs. Tasks with the same
    instance + kind + name (+ parameters, for processes) will share a
    signature.

    Chore signatures use a dedicated ``chore`` prefix so they occupy a
    disjoint hash space from existing process signatures. This means a
    chore named ``daily_etl`` and a process named ``daily_etl`` on the
    same instance never collide in the optimizer's history cache, even
    if they share a name by accident.

    :param instance: TM1 instance name
    :param process: TI process name (for process-kind tasks)
    :param parameters: Process parameters dictionary (ignored for chores)
    :param chore: TM1 chore name (for chore-kind tasks)
    :return: 16-character hex signature
    """
    if chore:
        # Chores have no invocation parameters — the dimension is just
        # (instance, chore_name). Prefix keeps the signature space
        # disjoint from process signatures.
        signature_input = f"chore|{instance}|{chore}"
    else:
        sorted_params = json.dumps(parameters, sort_keys=True) if parameters else "{}"
        signature_input = f"{instance}|{process or ''}|{sorted_params}"

    return hashlib.sha256(signature_input.encode()).hexdigest()[:16]
