"""Shared edgartools setup — initializes SEC EDGAR identity at import time."""

from __future__ import annotations

import os

import edgar

# SEC requires a User-Agent identity (name + email) for EDGAR access.
# edgartools uses module-level state, so calling set_identity() once at
# import time is sufficient for the entire process.
EDGAR_IDENTITY = os.environ.get("EDGAR_IDENTITY", os.environ.get("EDGAR_USER_AGENT", ""))
if EDGAR_IDENTITY:
    edgar.set_identity(EDGAR_IDENTITY)
