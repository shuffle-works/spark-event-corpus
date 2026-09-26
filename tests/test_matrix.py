from corpus.matrix import (
    BASELINE_CONFIG,
    COLD_START_DELAY_S,
    CACHE_SCENARIOS,
    FAILURE_SCENARIOS,
    PAIRWISE_SCENARIOS,
    baseline_runs,
    cache_runs,
    failure_runs,
    pairwise_runs,
)

EXPECTED_TAGS = {
    "SPEC", "HOST", "STRAG", "SKEW", "CACHE", "CSTOR",
    "PART", "TINY", "UTIL", "COLD", "SHFL", "SPILL",
}


def test_baseline_runs_covers_every_version_format_pair():
    runs = baseline_runs(["3.5.3", "4.0.0"])
    assert len(runs) == 6
    ids = {r.id for r in runs}
    assert "spark-3.5.3-parquet-baseline" in ids
    assert "spark-4.0.0-iceberg-baseline" in ids


def test_pairwise_scenarios_has_exactly_seven_rows():
    assert len(PAIRWISE_SCENARIOS) == 7


def test_pairwise_scenarios_drops_all_baseline_row():
    for scenario in PAIRWISE_SCENARIOS:
        assert scenario.config != BASELINE_CONFIG


def test_pairwise_scenarios_cover_every_expected_tag():
    """Every tag is either reached by some row or recorded as a known miss."""
    covered: set[str] = set()
    for scenario in PAIRWISE_SCENARIOS:
        covered.update(scenario.targets_detectors)
        covered.update(scenario.known_misses)
    assert covered == EXPECTED_TAGS


def test_known_misses_are_not_also_targets_and_say_why():
    for scenario in PAIRWISE_SCENARIOS:
        assert not set(scenario.known_misses) & set(scenario.targets_detectors), scenario.id
        assert all(reason.strip() for reason in scenario.known_misses.values()), scenario.id


def test_pairwise_runs_carry_their_known_misses():
    for run, scenario in zip(pairwise_runs("4.2.0"), PAIRWISE_SCENARIOS):
        assert run.known_misses == scenario.known_misses


def test_pairwise_rows_scale_the_cluster_and_the_map_stages():
    for scenario in PAIRWISE_SCENARIOS:
        assert scenario.config["third_worker"] is True
        assert scenario.config["spark_confs"]["spark.default.parallelism"] == "20"


def test_pairwise_confs_follow_the_targets_that_need_them():
    for scenario in PAIRWISE_SCENARIOS:
        confs = scenario.config["spark_confs"]
        config = scenario.config
        assert ("spark.speculation.minTaskRuntime" in confs) == config["speculation"]
        assert ("spark.eventLog.logBlockUpdates.enabled" in confs) == (config["caching"] != "none")
        assert (config.get("fact_source") == "parquet") == (config["caching"] != "none")
        assert (config.get("second_action") == "count") == (config["caching"] != "none")


def test_pairwise_cold_start_rows_delay_the_workers():
    delayed = {s.id for s in PAIRWISE_SCENARIOS if s.config.get("worker_start_delay_s")}
    assert delayed == {"pairwise-02", "pairwise-03", "pairwise-04", "pairwise-05"}
    assert all(
        s.config["worker_start_delay_s"] == COLD_START_DELAY_S
        for s in PAIRWISE_SCENARIOS if s.id in delayed
    )


def test_pairwise_runs_fills_in_latest_version():
    runs = pairwise_runs("4.1.2")
    assert all(r.spark_version == "4.1.2" for r in runs)
    assert all(r.table_format == "parquet" for r in runs)
    assert len(runs) == 7


def test_failure_scenarios_target_the_failure_detectors():
    covered = {tag for s in FAILURE_SCENARIOS for tag in s.targets_detectors}
    assert covered == {"RETRY", "FAIL", "SFAIL", "JOBS", "INCMP"}


def test_failure_scenarios_are_baseline_plus_one_failure_mode():
    modes = set()
    for scenario in FAILURE_SCENARIOS:
        config = dict(scenario.config)
        modes.add(config.pop("failure"))
        assert config == BASELINE_CONFIG
    assert modes == {"task-retry", "stage-abort", "job-failure", "killed"}


def test_failure_scenarios_stay_out_of_the_pairwise_matrix():
    assert all("failure" not in s.config for s in PAIRWISE_SCENARIOS)
    assert not {s.id for s in FAILURE_SCENARIOS} & {s.id for s in PAIRWISE_SCENARIOS}


def test_failure_runs_fills_in_latest_version():
    runs = failure_runs("4.2.0")
    assert [r.id for r in runs] == [s.id for s in FAILURE_SCENARIOS]
    assert all(r.spark_version == "4.2.0" and r.table_format == "parquet" for r in runs)


def test_cache_scenarios_cover_both_persist_modes_under_storage_pressure():
    modes = set()
    for scenario in CACHE_SCENARIOS:
        config = dict(scenario.config)
        modes.add(config.pop("caching"))
        assert config.pop("second_action") == "reread"
        assert config.pop("storage_pressure") is True
        assert config == {k: v for k, v in BASELINE_CONFIG.items() if k != "caching"}
        assert scenario.targets_detectors == ["CSTOR"]
    assert modes == {"memory-only", "memory-and-disk"}


def test_cache_scenarios_stay_out_of_the_other_scenario_lists():
    other_ids = {s.id for s in PAIRWISE_SCENARIOS + FAILURE_SCENARIOS}
    assert not {s.id for s in CACHE_SCENARIOS} & other_ids
    assert all("storage_pressure" not in s.config for s in PAIRWISE_SCENARIOS + FAILURE_SCENARIOS)


def test_cache_runs_fills_in_latest_version():
    runs = cache_runs("4.2.0")
    assert [r.id for r in runs] == [s.id for s in CACHE_SCENARIOS]
    assert all(r.spark_version == "4.2.0" and r.table_format == "parquet" for r in runs)
