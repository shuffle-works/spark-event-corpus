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
