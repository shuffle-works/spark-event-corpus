"""The single tag value shared by every catalog entry from one generation run."""
from __future__ import annotations

from datetime import date


def current_generation_tag() -> str:
    return f"v{date.today().isoformat()}"
