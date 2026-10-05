import os
import struct

import cramjam
import pytest

from corpus.validate import (
    InvalidEventLog,
    log_layout,
    validate_event_log,
    validate_ndjson_event_log,
)


def test_valid_log_returns_line_count(tmp_path):
    f = tmp_path / "log.ndjson"
    f.write_text(
        '{"Event": "SparkListenerApplicationStart"}\n'
        '{"Event": "SparkListenerApplicationEnd"}\n'
    )
    assert validate_ndjson_event_log(f) == 2


def test_malformed_json_raises(tmp_path):
    f = tmp_path / "log.ndjson"
    f.write_text('{"Event": "ok"}\nnot json\n')
    with pytest.raises(InvalidEventLog):
        validate_ndjson_event_log(f)


def test_missing_event_field_raises(tmp_path):
    f = tmp_path / "log.ndjson"
    f.write_text('{"NotEvent": "ok"}\n')
    with pytest.raises(InvalidEventLog):
        validate_ndjson_event_log(f)


def test_empty_file_raises(tmp_path):
    f = tmp_path / "log.ndjson"
    f.write_text("")
    with pytest.raises(InvalidEventLog):
        validate_ndjson_event_log(f)


def test_compressed_input_raises_invalid_event_log_not_unicode_error(tmp_path):
    """A zstd-compressed log (Spark's own default event-log encoding) must come
    back as InvalidEventLog, the exception callers actually catch -- not as the
    raw UnicodeDecodeError that escaping the decode would produce."""
    f = tmp_path / "log.ndjson"
    f.write_bytes(b"\x28\xb5\x2f\xfd" + os.urandom(64))
    with pytest.raises(InvalidEventLog):
        validate_ndjson_event_log(f)


# --- validate_event_log: what Spark 4 writes by default ---------------------

EVENTS = b'{"Event": "SparkListenerLogStart"}\n{"Event": "SparkListenerApplicationEnd"}\n'


def zstd_bytes(data: bytes) -> bytes:
    return bytes(cramjam.zstd.compress(data))


def lz4_block_bytes(data: bytes) -> bytes:
    """lz4-java's LZ4BlockOutputStream framing, as Spark's lz4 codec writes it."""
    packed = bytes(cramjam.lz4.compress_block(data, store_size=False))
    header = b"LZ4Block" + struct.pack("<BII", 0x20 | 0x07, len(packed), len(data)) + b"\0" * 4
    return header + packed


def snappy_block_bytes(data: bytes) -> bytes:
    """xerial's SnappyOutputStream framing, as Spark's snappy codec writes it."""
    packed = bytes(cramjam.snappy.compress_raw(data))
    return b"\x82SNAPPY\x00" + struct.pack(">ii", 1, 1) + struct.pack(">i", len(packed)) + packed


@pytest.mark.parametrize(
    "suffix, encode",
    [("", bytes), (".zstd", zstd_bytes), (".lz4", lz4_block_bytes), (".snappy", snappy_block_bytes)],
)
def test_validate_event_log_reads_every_spark_codec(tmp_path, suffix, encode):
    f = tmp_path / f"app-1{suffix}"
    f.write_bytes(encode(EVENTS))
    assert validate_event_log(f) == 2


def test_validate_event_log_rejects_a_corrupt_compressed_file(tmp_path):
    f = tmp_path / "app-1.zstd"
    f.write_bytes(b"\x28\xb5\x2f\xfd" + os.urandom(64))
    with pytest.raises(InvalidEventLog):
        validate_event_log(f)


def rolling_dir(tmp_path, names, encode=bytes):
    d = tmp_path / "eventlog_v2_app-1"
    d.mkdir()
    for name in names:
        (d / name).write_bytes(b"" if name.startswith("appstatus_") else encode(EVENTS))
    return d


def test_validate_event_log_sums_a_rolling_directory(tmp_path):
    d = rolling_dir(tmp_path, ["appstatus_app-1", "events_1_app-1.zstd", "events_2_app-1.zstd"], zstd_bytes)
    assert validate_event_log(d) == 4
    assert log_layout(d) == {"layout": "rolling-dir", "compression": "zstd"}


def test_log_layout_of_plain_and_compressed_single_files(tmp_path):
    assert log_layout(tmp_path / "x.ndjson") == {"layout": "single-file", "compression": "none"}
    assert log_layout(tmp_path / "x.lz4") == {"layout": "single-file", "compression": "lz4"}


@pytest.mark.parametrize(
    "names",
    [
        ["appstatus_app-1"],  # no event files
        ["events_1_app-1"],  # no appstatus marker
        ["appstatus_app-1", "events_1_app-1", "events_3_app-1"],  # a roll is missing
        ["appstatus_app-1", "events_1_app-1", "events_2_app-1.zstd"],  # mixed codecs
    ],
)
def test_validate_event_log_rejects_malformed_rolling_directories(tmp_path, names):
    d = rolling_dir(tmp_path, names)
    with pytest.raises(InvalidEventLog):
        validate_event_log(d)
