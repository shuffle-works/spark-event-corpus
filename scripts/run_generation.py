#!/usr/bin/env python3
"""Runs the full baseline + pairwise generation matrix, plus the standalone
failure, cache, event-log and Delta DML scenarios, end-to-end: take the pinned Spark versions
(src/corpus/versions.py) -> run each baseline/scenario via Docker Compose ->
validate the produced log -> copy it into the data repo -> append a catalog entry.
Committing/tagging the data repo happens once, by hand, after this finishes
(see the plan's Task 11), not per-run.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from corpus.catalog import append_entry, load_catalog, log_relpath, sha256_of, size_of
from corpus.matrix import (
    Run,
    baseline_runs,
    cache_runs,
    dml_runs,
    event_log_runs,
    failure_runs,
    pairwise_runs,
)
from corpus.orchestration import compose_services_for, env_for_run, expected_submit_exit_code
from corpus.table_formats import UnknownTableFormatMapping
from corpus.tagging import generation_tag_for
from corpus.validate import log_layout, validate_event_log
from corpus.versions import (
    SPARK_VERSIONS,
    VersionResolutionError,
    resolve_versions,
    scenario_spark_version,
    upstream_report,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_REPO = REPO_ROOT.parent / "spark-event-corpus-data"
CATALOG_PATH = REPO_ROOT / "index.json"


def run_one(run: Run, event_log_dir: Path, workload_output_dir: Path) -> Path:
    env = {**os.environ, **env_for_run(run, event_log_dir, workload_output_dir)}
    subprocess.run(
        ["docker", "compose", "up", "-d", *compose_services_for(run)],
        cwd=REPO_ROOT, env=env, check=True,
    )
    try:
        # The killed-run scenario halts its driver on purpose, so it has to
        # exit with that code, not 0; anything else means it failed some
        # other way.
        expected_exit = expected_submit_exit_code(run)
        submit = subprocess.run(
            ["docker", "compose", "run", "--rm", "spark-submit"], cwd=REPO_ROOT, env=env
        )
        if submit.returncode != expected_exit:
            raise RuntimeError(
                f"{run.id}: spark-submit exited {submit.returncode}, expected {expected_exit}"
            )
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
        # workload_output_dir only exists as a host bind mount so Delta's
        # post-commit read-back can see it from every container (see the
        # comment where it's created in main()); once the containers are
        # gone there's no reason to keep the table data it collected on the
        # host, at ROW_COUNT=5_000_000 real runs this is real Parquet/Delta/
        # Iceberg data, easily many GB across the full matrix. Must run
        # whether the run above succeeded or raised, so a mid-run failure
        # doesn't leak this directory either.
        shutil.rmtree(workload_output_dir, ignore_errors=True)

    # One file, or one eventlog_v2_<app> directory for a rolling log.
    produced = list(event_log_dir.glob("*"))
    if len(produced) != 1:
        raise RuntimeError(f"expected exactly one event log in {event_log_dir}, found {produced}")
    return produced[0]


def commit_run(run: Run, log_file: Path, tag: str, data_repo: Path) -> None:
    validate_event_log(log_file)
    relpath = log_relpath(run.id, log_file)
    dest = data_repo / relpath
    dest.parent.mkdir(parents=True, exist_ok=True)
    if log_file.is_dir():
        shutil.copytree(log_file, dest, dirs_exist_ok=True)
    else:
        shutil.copy(log_file, dest)
    layout = log_layout(dest)
    entry = {
        "id": run.id,
        "data_repo_tag": tag,
        "path": relpath,
        "checksum": sha256_of(dest),
        "source": "self-generated",
        "spark_version": run.spark_version,
        "table_format": run.table_format,
        "scenario": run.scenario,
        "config_diff": run.config,
        "targets_detectors": run.targets_detectors,
        **({"known_misses": run.known_misses} if run.known_misses else {}),
        "generated_at": tag[1:],
        "size_bytes": size_of(dest),
        # Recorded only for logs that are not one plain file, which is every
        # entry written before the event-log scenarios existed.
        **({"log_layout": layout["layout"], "compression": layout["compression"]}
           if layout != {"layout": "single-file", "compression": "none"} else {}),
    }
    append_entry(CATALOG_PATH, entry)


def check_upstream() -> None:
    try:
        lines = upstream_report(SPARK_VERSIONS, resolve_versions())
    except VersionResolutionError as exc:
        sys.exit(str(exc))
    print("\n".join(lines) if lines else "pinned Spark versions match the upstream listing")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", type=Path, default=Path("/tmp/spark-event-corpus-runs"))
    parser.add_argument(
        "--tag",
        help="data repo tag to stamp new entries with. Pass a fresh one when adding runs "
        "to a catalog whose existing logs are already tagged (default: the tag the "
        "catalog's entries already use, or today's date for an empty catalog)",
    )
    parser.add_argument(
        "--data-repo", type=Path, default=DEFAULT_DATA_REPO,
        help="spark-event-corpus-data clone to write the logs into (default: sibling of this repo)",
    )
    parser.add_argument(
        "--only", nargs="+", metavar="ID",
        help="run only these ids (e.g. eventlog-zstd); the rest are left alone, not skipped",
    )
    parser.add_argument(
        "--check-upstream", action="store_true",
        help="print how the pinned Spark versions differ from the Apache dist listing and "
        "exit; generates nothing",
    )
    args = parser.parse_args()
    if args.check_upstream:
        check_upstream()
        return
    # generated_at is derived from the tag, so it has to be a dated one.
    if args.tag and not re.fullmatch(r"v\d{4}-\d{2}-\d{2}", args.tag):
        parser.error(f"--tag must look like v2026-09-24, got {args.tag!r}")
    args.event_log_dir.mkdir(parents=True, exist_ok=True)
    # Derived from the catalog, not from today's date: this invocation may be
    # the Nth restart of a generation run that started on an earlier day, and
    # every entry from one run has to carry the single tag the data repo gets
    # by hand.
    tag = args.tag or generation_tag_for(CATALOG_PATH)

    scenario_version = scenario_spark_version()
    runs: list[Run] = (
        baseline_runs(SPARK_VERSIONS)
        + pairwise_runs(scenario_version)
        + failure_runs(scenario_version)
        + cache_runs(scenario_version)
        + event_log_runs(scenario_version)
        + dml_runs(scenario_version)
    )

    # Restarts must not re-attempt runs a prior invocation already finished
    # and cataloged: append_entry() raises ValueError on a duplicate id,
    # which would otherwise crash the whole script on the first repeat.
    # Loaded once per invocation, not per run, since a run can commit to the
    # catalog mid-loop.
    if args.only:
        unknown = set(args.only) - {run.id for run in runs}
        if unknown:
            parser.error(f"--only names no known run: {', '.join(sorted(unknown))}")
        runs = [run for run in runs if run.id in args.only]

    already_done = {entry["id"] for entry in load_catalog(CATALOG_PATH)}

    for run in runs:
        if run.id in already_done:
            print(f"SKIPPED (already done): {run.id}")
            continue
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
            commit_run(run, log_file, tag, args.data_repo)
        except UnknownTableFormatMapping as exc:
            print(f"SKIPPED: {run.id}: no upstream table-format artifact available yet ({exc})")
            # run_dir (and the workload-output dir under it) were created
            # above and nothing ever wrote to them, so drop them rather than
            # leave an empty directory per skipped run.
            shutil.rmtree(run_dir, ignore_errors=True)
            continue
        print(f"done: {run.id}")


if __name__ == "__main__":
    main()
