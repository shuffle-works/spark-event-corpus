"""Read/append/write index.json catalog entries."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def load_catalog(catalog_path: Path) -> list[dict[str, Any]]:
    if not catalog_path.exists():
        return []
    return json.loads(catalog_path.read_text())


def save_catalog(catalog_path: Path, entries: list[dict[str, Any]]) -> None:
    # Written via a same-directory temp file and an atomic rename, never
    # truncated in place: run_generation.py derives its whole resume decision
    # (already_done) from this file, so a crash mid-write would leave a
    # partial index.json that either silently means "redo everything" or
    # crashes the next invocation on json.JSONDecodeError.
    tmp_path = catalog_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(entries, indent=2, sort_keys=True) + "\n")
    tmp_path.replace(catalog_path)


def append_entry(catalog_path: Path, entry: dict[str, Any]) -> list[dict[str, Any]]:
    entries = load_catalog(catalog_path)
    existing_ids = {e["id"] for e in entries}
    if entry["id"] in existing_ids:
        raise ValueError(f"catalog already has an entry with id {entry['id']!r}")
    entries.append(entry)
    save_catalog(catalog_path, entries)
    return entries
