"""Read/append/write index.json catalog entries."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .validate import CODEC_SUFFIXES


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_of(path: Path) -> str:
    """Checksum of a log. For a file, its SHA-256. For a rolling event-log
    directory, the SHA-256 of one "<relative path>\\0<file sha256>\\n" line per
    file, sorted by path: it changes when any file or name does, and cannot be
    reproduced by hashing a single file."""
    if not path.is_dir():
        return f"sha256:{_file_digest(path)}"
    manifest = hashlib.sha256()
    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        manifest.update(f"{child.relative_to(path).as_posix()}\0{_file_digest(child)}\n".encode())
    return f"sha256:{manifest.hexdigest()}"


def log_relpath(run_id: str, produced: Path) -> str:
    """Where a produced log goes in the data repo, relative to its root.
    A plain file is <id>.ndjson, a compressed one keeps Spark's codec suffix
    (<id>.zstd), and a rolling directory keeps its eventlog_v2_<app> name
    under logs/<id>/."""
    if produced.is_dir():
        return f"logs/{run_id}/{produced.name}"
    return f"logs/{run_id}{produced.suffix if produced.suffix in CODEC_SUFFIXES else '.ndjson'}"


def size_of(path: Path) -> int:
    if not path.is_dir():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


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
