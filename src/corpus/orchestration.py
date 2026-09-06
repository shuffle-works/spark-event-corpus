"""Pure helpers for building the Docker Compose invocation for one run.
Kept free of subprocess/IO calls so the mapping logic is unit-testable;
scripts/run_generation.py does the actual subprocess calls."""
from __future__ import annotations

from pathlib import Path

from .matrix import Run
from .table_formats import artifact_for


def packages_for(run: Run) -> str:
    if run.table_format == "parquet":
        return ""
    minor_line = ".".join(run.spark_version.split(".")[:2])
    return f"--packages {artifact_for(minor_line, run.table_format)}"


def env_for_run(run: Run, event_log_dir: Path, row_count: int = 5_000_000) -> dict[str, str]:
    return {
        "SPARK_VERSION": run.spark_version,
        "AQE": str(run.config["aqe"]).lower(),
        "SHUFFLE_PARTITIONS": str(run.config["shuffle_partitions"]),
        "DYNAMIC_ALLOCATION": str(run.config["dynamic_allocation"]).lower(),
        "SPECULATION": str(run.config["speculation"]).lower(),
        "WORKER_2_CPU_LIMIT": "1" if run.config["slow_host"] else "2",
        "SKEW": run.config["skew"],
        "PERSIST_MODE": run.config["caching"],
        "TABLE_FORMAT": run.table_format,
        "ROW_COUNT": str(row_count),
        "PACKAGES_FLAG": packages_for(run),
        "EVENT_LOG_DIR": str(event_log_dir),
    }
