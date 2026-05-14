"""Shared shlex helpers for rushti parameter parsing.

TM1 process parameters never use ``\\`` as an escape character — it is always
a literal path separator (e.g. ``F:\\Cons\\Go_Files\\``). The default
``shlex.split(..., posix=True)`` treats backslash as an escape, which trips
on Windows-style paths and drops the whole parameter set.

Implementation note: ``shlex.split(..., posix=False)`` preserves backslashes
as literals but does *not* group whitespace-containing quoted strings
(``"Working Forecast"`` would split on the inner space). To get both
behaviors — literal backslashes AND quote-grouping — we pre-escape every
``\\`` in the input so POSIX mode preserves it, then tokenize with POSIX
semantics and strip the (already-consumed) outer quotes via the shlex
parser itself.
"""

from __future__ import annotations

import shlex
from typing import List


def shlex_split_literal_backslashes(text: str) -> List[str]:
    """Tokenize ``text`` like :func:`shlex.split` but treat backslash literally.

    Quote-grouping still applies: ``param="value with spaces"`` yields one
    token ``param=value with spaces``.

    :param text: The raw parameter string.
    :return: List of tokens with backslashes preserved.
    :raises ValueError: If quotes are unbalanced.
    """
    pre_escaped = text.replace("\\", "\\\\")
    return shlex.split(pre_escaped, posix=True)
