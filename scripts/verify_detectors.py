#!/usr/bin/env python3
"""Runs the pinned sparkforensics-cli over every self-generated log in the
catalog, records the detector tags that actually fire as fires_detectors next
to the intended targets_detectors in index.json, and exits non-zero when a
targeted detector does not fire.

Exit codes: 0 every target fired, 1 at least one target did not fire,
2 at least one log could not be checked (missing, checksum mismatch, or
analyzer failure). The catalog is rewritten in every case, so the observed
tags of the logs that were checked are kept either way.

Needs Node, for `npx`; the CLI itself is fetched from npm at the pinned
version on first use.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from corpus.catalog import load_catalog, save_catalog
from corpus.detectors import (
    CHECKED_WITH,
    AnalyzeError,
    Report,
    check_analyzer_exit,
    exit_code,
    verify_entries,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_REPO = REPO_ROOT.parent / "spark-event-corpus-data"
CATALOG_PATH = REPO_ROOT / "index.json"


def analyze(log_path: Path) -> Report:
    cmd = [
        "npx", "--yes", "--package", CHECKED_WITH,
        "sparkforensics-analyze", str(log_path), "--format", "json",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise AnalyzeError("npx not found: install Node to run sparkforensics-cli") from exc
    check_analyzer_exit(proc.returncode, proc.stderr)
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise AnalyzeError(f"sparkforensics-analyze printed invalid JSON: {exc}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--data-repo", type=Path, default=DEFAULT_DATA_REPO,
        help="spark-event-corpus-data clone holding the logs (default: sibling of this repo)",
    )
    args = parser.parse_args()

    entries = load_catalog(CATALOG_PATH)
    results = verify_entries(entries, args.data_repo, analyze)
    save_catalog(CATALOG_PATH, entries)

    by_id = {e["id"]: e for e in entries}
    for result in results:
        if result.error:
            print(f"UNCHECKED: {result.id}: {result.error}")
        elif result.missing:
            fires = ", ".join(by_id[result.id]["fires_detectors"]) or "none"
            print(f"MISSED: {result.id}: {', '.join(result.missing)} did not fire (fired: {fires})")
        else:
            print(f"ok: {result.id}")

    targeted = sum(len(by_id[r.id]["targets_detectors"]) for r in results if not r.error)
    missed = sum(len(r.missing) for r in results if r.missing)
    print(f"{targeted - missed} of {targeted} targeted detector tags fired ({CHECKED_WITH})")
    return exit_code(results)


if __name__ == "__main__":
    sys.exit(main())
