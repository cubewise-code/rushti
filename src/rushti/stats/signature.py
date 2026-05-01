"""Deterministic task-signature hashing.

Extracted from ``rushti.stats`` (formerly ``stats.py``) in Phase 3 of
the architecture refactor.
"""

import hashlib
import json
from typing import Any, Dict, Optional


def calculate_task_signature(
    instance: str, process: str, parameters: Optional[Dict[str, Any]]
) -> str:
    """Calculate deterministic signature for task identity.

    The signature uniquely identifies a task configuration for
    runtime estimation across multiple runs. Tasks with the same
    instance, process, and parameters will have the same signature.

    :param instance: TM1 instance name
    :param process: TI process name
    :param parameters: Process parameters dictionary
    :return: 16-character hex signature
    """
    # Sort parameters for deterministic hash
    sorted_params = json.dumps(parameters, sort_keys=True) if parameters else "{}"

    # Combine components
    signature_input = f"{instance}|{process}|{sorted_params}"

    # SHA256 hash (truncated for readability)
    return hashlib.sha256(signature_input.encode()).hexdigest()[:16]
