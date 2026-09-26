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

# Fact table rows and join keys for runs whose config does not set row_count
# or join_cardinality: every baseline, failure and cache run.
DEFAULT_ROW_COUNT = 5_000_000
DEFAULT_JOIN_CARDINALITY = 1000

# Worker-2's CPU quota and task slots on slow_host runs; see env_for_run.
SLOW_HOST_CPU_LIMIT = "0.3"
SLOW_HOST_CORES = "6"

# Spark confs for runs whose config sets storage_pressure (the cache
# scenarios). Block updates are off in the event log by default, and without
# them a log does not say which cached partitions ended up in memory, on disk,
# or nowhere. Compressed, the fact table caches into about 12 MB, which fits
# even in shrunken memory, so compression is off (about 37 MB) and unified
# memory drops to about 15 MB per executor: on Spark 4.2.0 that leaves 12 of
# 20 partitions cached under MEMORY_ONLY and all 20 on disk under
# MEMORY_AND_DISK. The parallelism splits the table into enough partitions
# for a partial cache to be measurable.
STORAGE_PRESSURE_CONFS = {
    "spark.eventLog.logBlockUpdates.enabled": "true",
    "spark.memory.fraction": "0.02",
    "spark.memory.storageFraction": "0.1",
    "spark.sql.inMemoryColumnarStorage.compressed": "false",
    "spark.default.parallelism": "20",
}


def failure_mode_for(run: Run) -> str:
    return run.config.get("failure", "none")


def expected_submit_exit_code(run: Run) -> int:
    """Every run's spark-submit exits 0 except the killed-run scenario, whose
    driver is halted on purpose. The other failure scenarios catch the failure
    they inject, so their driver still ends cleanly."""
    return KILLED_RUN_EXIT_CODE if failure_mode_for(run) == "killed" else 0


def second_action_for(run: Run) -> str:
    return run.config.get("second_action", "none")


def scenario_confs_for(run: Run) -> str:
    """--conf flags a scenario adds on top of compose.yaml's fixed set: the
    storage-pressure confs of the cache scenarios, then the row's own
    spark_confs (the pairwise rows'). Empty for every other run."""
    confs = dict(STORAGE_PRESSURE_CONFS) if run.config.get("storage_pressure", False) else {}
    confs.update(run.config.get("spark_confs", {}))
    return " ".join(f"--conf {key}={value}" for key, value in confs.items())


def compose_services_for(run: Run) -> list[str]:
    """The cluster services to bring up before spark-submit: two workers, or
    three for runs whose config sets third_worker."""
    services = ["spark-master", "spark-worker-1", "spark-worker-2"]
    if run.config.get("third_worker", False):
        services.append("spark-worker-3")
    return services


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


def env_for_run(run: Run, event_log_dir: Path, workload_output_dir: Path) -> dict[str, str]:
    """Every ${VAR} compose.yaml interpolates, in one dict. Callers should not
    need to graft extra keys on afterwards; tests/test_compose_contract.py
    asserts this stays exhaustive."""
    return {
        "SPARK_VERSION": run.spark_version,
        "AQE": str(run.config["aqe"]).lower(),
        "SHUFFLE_PARTITIONS": str(run.config["shuffle_partitions"]),
        "DYNAMIC_ALLOCATION": str(run.config["dynamic_allocation"]).lower(),
        "SPECULATION": str(run.config["speculation"]).lower(),
        # Worker-2 advertises WORKER_2_CORES slots whatever its quota (see
        # compose.yaml), so this quota throttles how fast it runs its tasks
        # rather than how many it is given. 6 slots on 0.3 CPU leaves each
        # task about a twentieth of a core: slow enough that HOST sees it on
        # the short tasks of a 2000-partition stage, and 6 tasks still running
        # at a stage's tail for speculation to copy.
        "WORKER_2_CPU_LIMIT": SLOW_HOST_CPU_LIMIT if run.config["slow_host"] else "2",
        "WORKER_2_CORES": SLOW_HOST_CORES if run.config["slow_host"] else "2",
        "WORKER_START_DELAY": str(run.config.get("worker_start_delay_s", 0)),
        "SKEW": run.config["skew"],
        "PERSIST_MODE": run.config["caching"],
        "FAILURE_MODE": failure_mode_for(run),
        "SECOND_ACTION": second_action_for(run),
        "SCENARIO_CONF_FLAGS": scenario_confs_for(run),
        "TABLE_FORMAT": run.table_format,
        "ROW_COUNT": str(run.config.get("row_count", DEFAULT_ROW_COUNT)),
        "PAYLOAD_COLUMNS": str(run.config.get("payload_columns", 0)),
        "JOIN_CARDINALITY": str(run.config.get("join_cardinality", DEFAULT_JOIN_CARDINALITY)),
        "FACT_SOURCE": run.config.get("fact_source", "range"),
        "PACKAGES_FLAG": packages_for(run),
        "TABLE_FORMAT_CONF_FLAGS": extra_confs_for(run),
        "EVENT_LOG_DIR": str(event_log_dir),
        "WORKLOAD_OUTPUT_DIR": str(workload_output_dir),
    }
