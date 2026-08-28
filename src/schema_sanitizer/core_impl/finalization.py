"""Small guards for cleanup paths executed during interpreter teardown.

It treats a missing or failing interpreter-finalization probe conservatively so teardown code
avoids unsafe blocking work.
"""

from __future__ import annotations

import sys


def runtime_is_finalizing() -> bool:
    """Return whether blocking cleanup should be skipped during Python shutdown."""
    probe = getattr(sys, "is_finalizing", None)
    if probe is None:
        return False
    try:
        return bool(probe())
    except BaseException:
        # A partially torn-down runtime is not a safe place to acquire process
        # locks, publish journals, join threads, or invoke extension callbacks.
        return True


__all__ = ["runtime_is_finalizing"]
