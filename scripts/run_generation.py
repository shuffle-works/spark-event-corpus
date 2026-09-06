#!/usr/bin/env python3
"""Runs the full baseline + pairwise generation matrix end-to-end: resolve
versions -> run each baseline/scenario via Docker Compose -> validate the
produced log -> copy it into the data repo -> append a catalog entry.
Committing/tagging the data repo happens once, by hand, after this finishes
(see the plan's Task 11), not per-run.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from corpus.catalog import append_entry, sha256_of
from corpus.matrix import Run, baseline_runs, pairwise_runs
from corpus.orchestration import env_for_run
from corpus.table_formats import UnknownTableFormatMapping
from corpus.tagging import current_generation_tag
from corpus.validate import validate_ndjson_event_log
from corpus.versions import resolve_versions

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_REPO = REPO_ROOT.parent / "spark-event-corpus-data"
CATALOG_PATH = REPO_ROOT / "index.json"
GENERATION_TAG = current_generation_tag()


def run_one(run: Run, event_log_dir: Path, workload_output_dir: Path) -> Path:
    env = {**os.environ, **env_for_run(run, event_log_dir), "WORKLOAD_OUTPUT_DIR": str(workload_output_dir)}
    subprocess.run(
        ["docker", "compose", "up", "-d", "spark-master", "spark-worker-1", "spark-worker-2"],
        cwd=REPO_ROOT, env=env, check=True,
    )
    try:
        subprocess.run(["docker", "compose", "run", "--rm", "spark-submit"], cwd=REPO_ROOT, env=env, check=True)
        # The apache/spark image runs as uid 185, so the event-log file it
        # writes into the EVENT_LOG_DIR bind mount comes out owned by uid/gid
        # 185, mode 660 -- unreadable by the host user that runs this script.
        # Fix it up from a throwaway root container over the same mount
        # (reusing the spark-submit service's image/volumes, entrypoint
        # overridden to chmod) so validate/copy below can read it back.
        subprocess.run(
            [
                "docker", "compose", "run", "--rm", "--no-deps", "--user", "root",
                "--entrypoint", "chmod", "spark-submit", "-R", "a+rwX", "/tmp/spark-events",
            ],
            cwd=REPO_ROOT, env=env, check=True,
        )
    finally:
        subprocess.run(["docker", "compose", "down"], cwd=REPO_ROOT, env=env, check=True)

    produced = [p for p in event_log_dir.glob("*") if p.is_file()]
    if len(produced) != 1:
        raise RuntimeError(f"expected exactly one event log in {event_log_dir}, found {produced}")
    return produced[0]


def commit_run(run: Run, log_file: Path) -> None:
    validate_ndjson_event_log(log_file)
    dest = DATA_REPO / "logs" / f"{run.id}.ndjson"
    shutil.copy(log_file, dest)
    entry = {
        "id": run.id,
        "data_repo_tag": GENERATION_TAG,
        "path": f"logs/{dest.name}",
        "checksum": sha256_of(dest),
        "source": "self-generated",
        "spark_version": run.spark_version,
        "table_format": run.table_format,
        "scenario": run.scenario,
        "config_diff": run.config,
        "targets_detectors": run.targets_detectors,
        "generated_at": GENERATION_TAG[1:],
        "size_bytes": dest.stat().st_size,
    }
    append_entry(CATALOG_PATH, entry)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", type=Path, default=Path("/tmp/spark-event-corpus-runs"))
    args = parser.parse_args()
    args.event_log_dir.mkdir(parents=True, exist_ok=True)

    versions = [str(v) for v in resolve_versions()]
    latest = versions[-1]
    runs: list[Run] = baseline_runs(versions) + pairwise_runs(latest)

    for run in runs:
        run_dir = args.event_log_dir / run.id
        run_dir.mkdir(parents=True, exist_ok=True)
        # apache/spark's container user (uid 185) needs to write into this
        # bind-mounted directory; a plain mkdir leaves it owned by the host
        # user with no other-write bit, which makes spark-submit fail with
        # FileNotFoundException: ... (Permission denied) on the event-log
        # file (confirmed in Task 8's manual verification).
        run_dir.chmod(0o777)
        # Iceberg's warehouse and the workload's --output-path both need to be
        # readable/writable from the driver (spark-submit) and the worker
        # containers alike -- Delta's post-commit step schedules a
        # distributed re-read of a _delta_log file the driver just wrote
        # locally, which fails unless this is a shared mount (see compose.yaml).
        # Nested under run_dir so it stays out of the event-log glob below.
        workload_output_dir = run_dir / "workload-output"
        workload_output_dir.mkdir(parents=True, exist_ok=True)
        workload_output_dir.chmod(0o777)
        try:
            log_file = run_one(run, run_dir, workload_output_dir)
            commit_run(run, log_file)
        except UnknownTableFormatMapping as exc:
            print(f"SKIPPED: {run.id}: no upstream table-format artifact available yet ({exc})")
            continue
        print(f"done: {run.id}")


if __name__ == "__main__":
    main()
