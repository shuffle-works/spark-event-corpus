#!/usr/bin/env python3
"""Fetches publicly-reachable Spark event logs from a curated source list,
validates each, and appends catalog entries. Confirmed sources only: Apache
Spark's own bundled test-resource event logs and khodosko/sparkDoctor's
fixture set. Both are Apache-2.0. Real logs from real Spark runs are the point
-- they cover engine versions and workload shapes the generated matrix does
not.

Safe to re-invoke after a partial or failed run: entries already in the
catalog are skipped before any clone-validate-copy work happens.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from corpus.catalog import append_entry, load_catalog, sha256_of
from corpus.tagging import generation_tag_for
from corpus.validate import InvalidEventLog, validate_ndjson_event_log

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_REPO = REPO_ROOT.parent / "spark-event-corpus-data"
CATALOG_PATH = REPO_ROOT / "index.json"
EXTERNAL_LOG_DIR = DATA_REPO / "logs" / "external"
# See run_generation.py: derived from the catalog so a restart stays on the
# one tag the data repo actually gets.
GENERATION_TAG = generation_tag_for(CATALOG_PATH)

SOURCES = [
    {
        "repo": "https://github.com/apache/spark.git",
        "subpath": "core/src/test/resources/spark-events",
        "license": "Apache-2.0",
        "branch": "master",
    },
    {
        "repo": "https://github.com/khodosko/sparkDoctor.git",
        "subpath": "src/test/resources/fixtures",
        "license": "Apache-2.0",
        # khodosko/sparkDoctor's default branch is "main", not "master"
        # (confirmed via the GitHub API and by the fact that a
        # .../tree/master/... URL 302-redirects to .../tree/main/...):
        # unlike apache/spark, this repo can't share the same hardcoded
        # branch name in its source_url.
        "branch": "main",
    },
]


def sparse_checkout(repo_url: str, subpath: str, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=dest, check=True)
    subprocess.run(["git", "remote", "add", "origin", repo_url], cwd=dest, check=True)
    subprocess.run(["git", "sparse-checkout", "set", subpath], cwd=dest, check=True)
    subprocess.run(["git", "pull", "--depth", "1", "origin", "HEAD"], cwd=dest, check=True)


def main() -> None:
    EXTERNAL_LOG_DIR.mkdir(parents=True, exist_ok=True)
    # Loaded once per invocation, as in run_generation.py: append_entry()
    # raises ValueError on a duplicate id, so without this a second run would
    # abort on the first already-fetched log -- after having already
    # overwritten the destination file.
    already_done = {entry["id"] for entry in load_catalog(CATALOG_PATH)}

    for source in SOURCES:
        repo_name = source["repo"].rsplit("/", 1)[-1].removesuffix(".git")
        checkout_dir = Path(tempfile.mkdtemp(prefix=f"{repo_name}-checkout-"))
        try:
            sparse_checkout(source["repo"], source["subpath"], checkout_dir)

            found_dir = checkout_dir / source["subpath"]
            for log_file in sorted(found_dir.iterdir()):
                if not log_file.is_file():
                    continue
                entry_id = f"external-{log_file.stem}"
                if entry_id in already_done:
                    print(f"SKIPPED (already done): {entry_id}")
                    continue
                try:
                    validate_ndjson_event_log(log_file)
                except InvalidEventLog as exc:
                    print(f"skipping {log_file}: {exc}")
                    continue

                dest_name = f"{entry_id}.ndjson"
                dest = EXTERNAL_LOG_DIR / dest_name
                shutil.copy(log_file, dest)
                entry = {
                    "id": entry_id,
                    "data_repo_tag": GENERATION_TAG,
                    "path": f"logs/external/{dest_name}",
                    "checksum": sha256_of(dest),
                    "source": "external",
                    "source_url": f"{source['repo'].removesuffix('.git')}/tree/{source['branch']}/{source['subpath']}/{log_file.name}",
                    "license": source["license"],
                    "fetched_at": GENERATION_TAG[1:],
                    "size_bytes": dest.stat().st_size,
                }
                append_entry(CATALOG_PATH, entry)
                print(f"added {entry['id']}")
        finally:
            # A validation failure mid-source must not leak the checkout.
            shutil.rmtree(checkout_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
