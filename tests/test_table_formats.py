import pytest

from corpus.table_formats import artifact_for, UnknownTableFormatMapping


def test_artifact_for_known_version():
    assert artifact_for("3.5", "delta") == "io.delta:delta-spark_2.12:3.2.0"
    assert artifact_for("3.5", "iceberg").startswith(
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:"
    )


def test_artifact_for_unknown_version_fails_loudly():
    with pytest.raises(UnknownTableFormatMapping):
        artifact_for("9.9", "delta")


def test_artifact_for_spark_4_0_delta():
    assert artifact_for("4.0", "delta") == "io.delta:delta-spark_4.0_2.13:4.4.0"


def test_artifact_for_spark_4_2_iceberg_has_no_mapping_yet():
    # Iceberg has not published a Spark 4.2 runtime as of its latest release
    # (1.11.0); this must keep failing loudly, not silently skip coverage.
    with pytest.raises(UnknownTableFormatMapping):
        artifact_for("4.2", "iceberg")
