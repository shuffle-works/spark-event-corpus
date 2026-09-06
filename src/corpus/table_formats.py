"""Spark version -> Delta/Iceberg Maven artifact coordinates.

New entries must be added by hand as new Spark minors get resolved; there is
no way to derive a compatible Delta/Iceberg version automatically, so an
unmapped version fails loudly instead of silently skipping table-format
coverage for that version. Before adding an entry for a newly-resolved Spark
minor, check https://docs.delta.io/latest/releases.html and
https://iceberg.apache.org/multi-engine-support/: both mappings go stale
independently of this repo's own release cadence.
"""
from __future__ import annotations


class UnknownTableFormatMapping(RuntimeError):
    pass


# minor line "X.Y" -> {"delta": "<maven coord>", "iceberg": "<maven coord>"}
TABLE_FORMAT_ARTIFACTS: dict[str, dict[str, str]] = {
    "3.5": {
        "delta": "io.delta:delta-spark_2.12:3.2.0",
        "iceberg": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    },
    # Spark 4.0 dropped Scala 2.12 support (SPARK-45314), so every Spark 4.x
    # artifact below uses the _2.13 suffix instead of 3.5's _2.12.
    "4.0": {
        "delta": "io.delta:delta-spark_4.0_2.13:4.4.0",
        "iceberg": "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
    },
    "4.1": {
        "delta": "io.delta:delta-spark_4.1_2.13:4.4.0",
        "iceberg": "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
    },
    "4.2": {
        "delta": "io.delta:delta-spark_4.2_2.13:4.4.0",
        # No "iceberg" key: Iceberg's latest release (1.11.0) has not yet
        # published a Spark 4.2 runtime artifact. artifact_for("4.2",
        # "iceberg") is expected to raise UnknownTableFormatMapping until
        # upstream Iceberg ships one.
    },
}


def artifact_for(minor_line: str, table_format: str) -> str:
    try:
        return TABLE_FORMAT_ARTIFACTS[minor_line][table_format]
    except KeyError as exc:
        raise UnknownTableFormatMapping(
            f"no known {table_format} artifact for Spark {minor_line}.x; check "
            f"https://docs.delta.io/latest/releases.html and "
            f"https://iceberg.apache.org/multi-engine-support/ and add an "
            f"entry to TABLE_FORMAT_ARTIFACTS"
        ) from exc
