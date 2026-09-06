import pytest

from corpus.versions import (
    SparkVersion,
    VersionResolutionError,
    parse_versions,
    latest_patch_per_minor,
    SPARK_DIST_URL,
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
