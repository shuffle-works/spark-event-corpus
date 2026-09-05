from corpus.matrix import baseline_runs, pairwise_runs, PAIRWISE_SCENARIOS, BASELINE_CONFIG

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
