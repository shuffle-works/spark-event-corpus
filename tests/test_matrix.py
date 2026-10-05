from corpus.matrix import (
    BASELINE_CONFIG,
    DML_SCENARIOS,
    EVENT_LOG_SCENARIOS,
    COLD_START_DELAY_S,
    CACHE_SCENARIOS,
    FAILURE_SCENARIOS,
    PAIRWISE_SCENARIOS,
    baseline_runs,
    cache_runs,
    dml_runs,
    event_log_runs,
    failure_runs,
    pairwise_runs,
)

EXPECTED_TAGS = {
    "SPEC", "HOST", "STRAG", "SKEW", "CACHE", "CSTOR",
    "PART", "TINY", "UTIL", "COLD", "SHFL", "SPILL",
}


def test_baseline_runs_covers_every_version_format_pair():
    runs = baseline_runs({"3.5": "3.5.3", "4.0": "4.0.0"})
    assert len(runs) == 6
    ids = {r.id for r in runs}
    assert "spark-3.5-parquet-baseline" in ids
    assert "spark-4.0-iceberg-baseline" in ids


def test_baseline_ids_are_keyed_by_minor_line_and_keep_the_patch_as_data():
    by_id = {r.id: r for r in baseline_runs({"4.1": "4.1.3"})}
    assert by_id["spark-4.1-delta-baseline"].spark_version == "4.1.3"
    # A patch bump changes the version, not the id a consumer hard-codes.
    bumped = {r.id for r in baseline_runs({"4.1": "4.1.4"})}
    assert bumped == set(by_id)


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


def test_event_log_scenarios_cover_every_codec_and_the_rolled_layouts():
    by_id = {s.id: s.config["event_log_confs"] for s in EVENT_LOG_SCENARIOS}
    assert set(by_id) == {
        "eventlog-spark4-default", "eventlog-rolling",
        "eventlog-zstd", "eventlog-lz4", "eventlog-snappy",
    }
    for codec in ("zstd", "lz4", "snappy"):
        assert by_id[f"eventlog-{codec}"]["spark.eventLog.compression.codec"] == codec
        assert by_id[f"eventlog-{codec}"]["spark.eventLog.rolling.enabled"] == "false"
    # The default run leaves rolling and compression to Spark; the rolled runs set a roll size.
    assert "spark.eventLog.rolling.enabled" not in by_id["eventlog-spark4-default"]
    assert "spark.eventLog.compress" not in by_id["eventlog-spark4-default"]
    assert by_id["eventlog-rolling"]["spark.eventLog.rolling.enabled"] == "true"
    assert "spark.eventLog.rolling.maxFileSize" in by_id["eventlog-rolling"]


def test_event_log_runs_use_the_scenario_version_and_target_no_detectors():
    runs = event_log_runs("4.2.0")
    assert len(runs) == 5
    assert all(r.spark_version == "4.2.0" and r.table_format == "parquet" for r in runs)
    assert all(r.targets_detectors == [] for r in runs)


def test_dml_scenarios_cover_merge_update_delete_and_concurrent_merges():
    modes = {s.config["dml"] for s in DML_SCENARIOS}
    assert modes == {"merge-sql", "merge-api", "update", "delete", "concurrent-merge"}
    assert {s.id for s in DML_SCENARIOS} == {f"delta-{m}" for m in modes}


def test_dml_runs_are_delta_runs_on_the_scenario_version():
    runs = dml_runs("4.2.0")
    assert len(runs) == 5
    assert all(r.spark_version == "4.2.0" and r.table_format == "delta" for r in runs)
    assert all(r.config["row_count"] == 1_000_000 for r in runs)


def test_dml_scenarios_stay_out_of_the_other_families():
    dml_ids = {s.id for s in DML_SCENARIOS}
    others = {s.id for s in PAIRWISE_SCENARIOS + FAILURE_SCENARIOS + CACHE_SCENARIOS + EVENT_LOG_SCENARIOS}
    assert not dml_ids & others
