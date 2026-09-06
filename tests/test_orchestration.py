from pathlib import Path

from corpus.matrix import Run
from corpus.orchestration import packages_for, env_for_run


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


def test_env_for_run_maps_slow_host_to_worker_cpu_limit():
    env = env_for_run(make_run(slow_host=True), Path("/tmp/x"))
    assert env["WORKER_2_CPU_LIMIT"] == "1"
    env2 = env_for_run(make_run(slow_host=False), Path("/tmp/x"))
    assert env2["WORKER_2_CPU_LIMIT"] == "2"
