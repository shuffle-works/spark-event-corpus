import pytest

from corpus.versions import (
    SparkVersion,
    VersionResolutionError,
    parse_versions,
    latest_patch_per_minor,
    SPARK_DIST_URL,
    SPARK_VERSIONS,
    SCENARIO_SPARK_LINE,
    scenario_spark_version,
    upstream_report,
)

SAMPLE_LISTING = """
<html><body>
<a href="spark-3.5.1/">spark-3.5.1/</a>
<a href="spark-3.5.3/">spark-3.5.3/</a>
<a href="spark-4.0.0/">spark-4.0.0/</a>
<a href="spark-4.1.2/">spark-4.1.2/</a>
<a href="spark-4.1.0/">spark-4.1.0/</a>
</body></html>
"""


def test_parse_versions_extracts_all_directories():
    versions = parse_versions(SAMPLE_LISTING)
    assert SparkVersion(3, 5, 1) in versions
    assert SparkVersion(4, 1, 2) in versions
    assert len(versions) == 5


def test_parse_versions_raises_on_empty_listing():
    with pytest.raises(VersionResolutionError):
        parse_versions("<html><body>no versions here</body></html>")


def test_latest_patch_per_minor_picks_highest_patch():
    versions = parse_versions(SAMPLE_LISTING)
    latest = latest_patch_per_minor(versions)
    assert SparkVersion(3, 5, 3) in latest
    assert SparkVersion(3, 5, 1) not in latest
    assert SparkVersion(4, 1, 2) in latest
    assert len(latest) == 3


def test_spark_dist_url_uses_live_mirror():
    """Regression test: ensure SPARK_DIST_URL points to live releases, not archived history."""
    assert "archive.apache.org" not in SPARK_DIST_URL
    assert SPARK_DIST_URL == "https://downloads.apache.org/spark/"


def test_pinned_versions_are_keyed_by_their_own_minor_line():
    for line, patch in SPARK_VERSIONS.items():
        assert patch.startswith(f"{line}.")
    assert SCENARIO_SPARK_LINE in SPARK_VERSIONS
    assert scenario_spark_version() == SPARK_VERSIONS[SCENARIO_SPARK_LINE]


def test_upstream_report_is_empty_when_pins_match():
    upstream = parse_versions(SAMPLE_LISTING)
    assert upstream_report({"3.5": "3.5.3", "4.0": "4.0.0", "4.1": "4.1.2"}, upstream) == []


def test_upstream_report_lists_newer_patch_unpinned_minor_and_missing_pin():
    upstream = parse_versions(SAMPLE_LISTING)
    pinned = {"3.5": "3.5.1", "4.1": "4.1.2", "3.4": "3.4.9"}
    report = upstream_report(pinned, upstream)
    assert "3.5: pinned 3.5.1, upstream has 3.5.3" in report
    assert "3.4: pinned 3.4.9 is not on the dist listing" in report
    assert "4.0: not pinned, upstream has 4.0.0" in report
    assert not any(line.startswith("4.1") for line in report)
