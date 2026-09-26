from pathlib import Path

from corpus.matrix import Run
from corpus.orchestration import (
    DEFAULT_ROW_COUNT,
    KILLED_RUN_EXIT_CODE,
    compose_services_for,
    env_for_run,
    expected_submit_exit_code,
    extra_confs_for,
    packages_for,
    scenario_confs_for,
)


def make_run(table_format="parquet", **config_overrides):
    config = {
        "aqe": True, "shuffle_partitions": 200, "dynamic_allocation": True,
        "speculation": False, "slow_host": False, "skew": "none", "caching": "none",
    }
    config.update(config_overrides)
    return Run(id="test", spark_version="3.5.3", table_format=table_format,
                scenario="baseline", config=config)


def test_packages_for_parquet_is_empty():
    assert packages_for(make_run("parquet")) == ""


def test_packages_for_delta_uses_known_artifact():
    assert packages_for(make_run("delta")) == "--packages io.delta:delta-spark_2.12:3.2.0"


def test_extra_confs_for_parquet_is_empty():
    assert extra_confs_for(make_run("parquet")) == ""


def test_extra_confs_for_delta_configures_extension_and_catalog():
    assert extra_confs_for(make_run("delta")) == (
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )


def test_extra_confs_for_iceberg_configures_extension_and_catalog():
    assert extra_confs_for(make_run("iceberg")) == (
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog "
        "--conf spark.sql.catalog.local.type=hadoop "
        "--conf spark.sql.catalog.local.warehouse=/tmp/spark-workload-output/iceberg-warehouse"
    )


def test_env_for_run_maps_slow_host_to_worker_2_quota_and_slots():
    env = env_for_run(make_run(slow_host=True), Path("/tmp/x"), Path("/tmp/y"))
    assert (env["WORKER_2_CPU_LIMIT"], env["WORKER_2_CORES"]) == ("0.3", "6")
    env2 = env_for_run(make_run(slow_host=False), Path("/tmp/x"), Path("/tmp/y"))
    assert (env2["WORKER_2_CPU_LIMIT"], env2["WORKER_2_CORES"]) == ("2", "2")


def test_env_for_run_defaults_the_workload_scale_to_the_baseline():
    env = env_for_run(make_run(), Path("/tmp/x"), Path("/tmp/y"))
    assert env["ROW_COUNT"] == str(DEFAULT_ROW_COUNT)
    assert env["PAYLOAD_COLUMNS"] == "0"
    assert env["JOIN_CARDINALITY"] == "1000"
    assert env["FACT_SOURCE"] == "range"
    assert env["WORKER_START_DELAY"] == "0"


def test_env_for_run_passes_a_rows_scale_knobs_through():
    run = make_run(
        row_count=16_000_000, payload_columns=8, join_cardinality=4,
        fact_source="parquet", second_action="count", worker_start_delay_s=45,
    )
    env = env_for_run(run, Path("/tmp/x"), Path("/tmp/y"))
    assert env["ROW_COUNT"] == "16000000"
    assert env["PAYLOAD_COLUMNS"] == "8"
    assert env["JOIN_CARDINALITY"] == "4"
    assert env["FACT_SOURCE"] == "parquet"
    assert env["SECOND_ACTION"] == "count"
    assert env["WORKER_START_DELAY"] == "45"


def test_compose_services_for_adds_the_third_worker_only_when_asked():
    assert compose_services_for(make_run()) == ["spark-master", "spark-worker-1", "spark-worker-2"]
    assert compose_services_for(make_run(third_worker=True))[-1] == "spark-worker-3"


def test_env_for_run_includes_both_bind_mount_dirs():
    env = env_for_run(make_run(), Path("/tmp/x"), Path("/tmp/y"))
    assert env["EVENT_LOG_DIR"] == "/tmp/x"
    assert env["WORKLOAD_OUTPUT_DIR"] == "/tmp/y"


def test_env_for_run_defaults_failure_mode_to_none():
    env = env_for_run(make_run(), Path("/tmp/x"), Path("/tmp/y"))
    assert env["FAILURE_MODE"] == "none"
    env2 = env_for_run(make_run(failure="stage-abort"), Path("/tmp/x"), Path("/tmp/y"))
    assert env2["FAILURE_MODE"] == "stage-abort"


def test_only_the_killed_run_expects_a_nonzero_submit_exit():
    assert expected_submit_exit_code(make_run()) == 0
    assert expected_submit_exit_code(make_run(failure="job-failure")) == 0
    assert expected_submit_exit_code(make_run(failure="killed")) == KILLED_RUN_EXIT_CODE



def test_env_for_run_defaults_to_no_second_action_and_no_scenario_confs():
    env = env_for_run(make_run(), Path("/tmp/x"), Path("/tmp/y"))
    assert env["SECOND_ACTION"] == "none"
    assert env["SCENARIO_CONF_FLAGS"] == ""


def test_scenario_confs_render_a_rows_spark_confs():
    run = make_run(spark_confs={"spark.default.parallelism": "20", "spark.memory.fraction": "0.02"})
    assert scenario_confs_for(run) == (
        "--conf spark.default.parallelism=20 --conf spark.memory.fraction=0.02"
    )


def test_storage_pressure_logs_block_updates_and_shrinks_storage_memory():
    run = make_run(caching="memory-and-disk", second_action="reread", storage_pressure=True)
    flags = scenario_confs_for(run)
    assert "--conf spark.eventLog.logBlockUpdates.enabled=true" in flags
    assert "--conf spark.memory.fraction=" in flags
    assert "--conf spark.memory.storageFraction=" in flags
    env = env_for_run(run, Path("/tmp/x"), Path("/tmp/y"))
    assert env["SCENARIO_CONF_FLAGS"] == flags
    assert env["SECOND_ACTION"] == "reread"
    assert env["PERSIST_MODE"] == "memory-and-disk"
