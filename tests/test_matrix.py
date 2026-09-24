from corpus.matrix import (
    BASELINE_CONFIG,
    FAILURE_SCENARIOS,
    PAIRWISE_SCENARIOS,
    baseline_runs,
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
    covered: set[str] = set()
    for scenario in PAIRWISE_SCENARIOS:
        covered.update(scenario.targets_detectors)
    assert covered == EXPECTED_TAGS


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
