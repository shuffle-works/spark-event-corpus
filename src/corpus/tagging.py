"""The single tag value shared by every catalog entry from one generation run."""
from __future__ import annotations

from datetime import date
from pathlib import Path


def current_generation_tag() -> str:
    return f"v{date.today().isoformat()}"


def generation_tag_for(catalog_path: Path) -> str:
    """The tag this invocation should stamp new catalog entries with: whatever
    tag the catalog's existing entries already use (so a resumed/restarted run
    stays consistent with entries a prior invocation already committed), or a
    fresh tag if the catalog is empty or doesn't exist yet.

    Without this, a generation run restarted on a later calendar day would
    stamp its remaining entries with a second date-based tag that has no
    matching git tag in the data repo -- the data repo gets exactly one
    hand-applied tag, at the end.
    """
    # Imported inside the function to keep this module's import graph a leaf:
    # catalog.py does not import tagging today, so there is no cycle to break,
    # but a local import means adding one later can't create one either.
    from .catalog import load_catalog

    entries = load_catalog(catalog_path)
    if entries:
        return entries[0]["data_repo_tag"]
    return current_generation_tag()
