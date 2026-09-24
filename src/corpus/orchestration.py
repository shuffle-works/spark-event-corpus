"""Pure helpers for building the Docker Compose invocation for one run.
Kept free of subprocess/IO calls so the mapping logic is unit-testable;
scripts/run_generation.py does the actual subprocess calls."""
from __future__ import annotations

from pathlib import Path

from .matrix import Run
from .table_formats import artifact_for

# What spark-submit exits with on the "killed" failure scenario: the workload
# halts the driver JVM with this code (the same one a SIGKILL produces).
# workload/generate_events.py cannot import this package, so it repeats the
# value; generation fails if the two drift, because run_generation.py checks
# the exit code.
KILLED_RUN_EXIT_CODE = 137


def failure_mode_for(run: Run) -> str:
    return run.config.get("failure", "none")


def expected_submit_exit_code(run: Run) -> int:
    """Every run's spark-submit exits 0 except the killed-run scenario, whose
    driver is halted on purpose. The other failure scenarios catch the failure
    they inject, so their driver still ends cleanly."""
    return KILLED_RUN_EXIT_CODE if failure_mode_for(run) == "killed" else 0


def packages_for(run: Run) -> str:
    if run.table_format == "parquet":
        return ""
    minor_line = ".".join(run.spark_version.split(".")[:2])
    return f"--packages {artifact_for(minor_line, run.table_format)}"


def extra_confs_for(run: Run) -> str:
    """--conf flags for the Spark SQL extension/catalog each table format
    needs to actually read/write, beyond the --packages jar. Parquet needs
    neither."""
    if run.table_format == "delta":
        return (
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    if run.table_format == "iceberg":
        return (
            "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
            "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog "
            "--conf spark.sql.catalog.local.type=hadoop "
            "--conf spark.sql.catalog.local.warehouse=/tmp/spark-workload-output/iceberg-warehouse"
        )
    return ""


def env_for_run(
    run: Run,
    event_log_dir: Path,
    workload_output_dir: Path,
    row_count: int = 5_000_000,
) -> dict[str, str]:
    """Every ${VAR} compose.yaml interpolates, in one dict. Callers should not
    need to graft extra keys on afterwards; tests/test_compose_contract.py
    asserts this stays exhaustive."""
    return {
        "SPARK_VERSION": run.spark_version,
        "AQE": str(run.config["aqe"]).lower(),
        "SHUFFLE_PARTITIONS": str(run.config["shuffle_partitions"]),
        "DYNAMIC_ALLOCATION": str(run.config["dynamic_allocation"]).lower(),
        "SPECULATION": str(run.config["speculation"]).lower(),
        # Both workers advertise 2 cores unconditionally (see compose.yaml), so
        # this quota throttles how fast worker-2 runs its tasks rather than how
        # many it is given. 0.5 CPU against 2 concurrent task slots is real
        # contention; the old "1" merely made the worker advertise 1 core and
        # left per-task speed untouched.
        "WORKER_2_CPU_LIMIT": "0.5" if run.config["slow_host"] else "2",
        "SKEW": run.config["skew"],
        "PERSIST_MODE": run.config["caching"],
        "FAILURE_MODE": failure_mode_for(run),
        "TABLE_FORMAT": run.table_format,
        "ROW_COUNT": str(row_count),
        "PACKAGES_FLAG": packages_for(run),
        "TABLE_FORMAT_CONF_FLAGS": extra_confs_for(run),
        "EVENT_LOG_DIR": str(event_log_dir),
        "WORKLOAD_OUTPUT_DIR": str(workload_output_dir),
    }
