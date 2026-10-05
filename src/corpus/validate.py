"""Lightweight structural check that a file is a well-formed Spark NDJSON
event log: every non-blank line is valid JSON with an 'Event' field. This is
deliberately not a full parse against sparkforensics' own TypeScript parser:
that would pull a JS toolchain into a Python repo for a check this simple
substitutes for.

validate_ndjson_event_log accepts plain text only; fetch_external.py relies on
that to skip non-log files from upstream sources. validate_event_log is the
format-aware check for what Spark itself writes: plain, codec-compressed
(zstd, lz4, snappy) and rolling-directory logs.
"""
from __future__ import annotations

import json
import re
import struct
from pathlib import Path

import cramjam


class InvalidEventLog(ValueError):
    pass


def validate_ndjson_event_log(path: Path) -> int:
    line_count = 0
    # Binary/compressed input decodes, not parses, so it fails with
    # UnicodeDecodeError -- a ValueError, but not an InvalidEventLog, so
    # callers' `except InvalidEventLog` would not catch it and one non-UTF-8
    # file in an upstream source would abort the whole fetch with a traceback
    # instead of the clean "skipping ..." message this validator exists for.
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise InvalidEventLog(f"{path}:{line_no}: not valid JSON: {exc}") from exc
                if "Event" not in record:
                    raise InvalidEventLog(f"{path}:{line_no}: missing required 'Event' field")
                line_count += 1
    except UnicodeDecodeError as exc:
        raise InvalidEventLog(f"{path}: not plain-text UTF-8 (is it compressed?): {exc}") from exc
    if line_count == 0:
        raise InvalidEventLog(f"{path}: no event lines found")
    return line_count


# Spark's event-log codec short name -> the file suffix it appends.
CODEC_SUFFIXES = {".zstd": "zstd", ".lz4": "lz4", ".snappy": "snappy"}

# Spark wraps lz4 and snappy in the framing of the JVM libraries it uses
# (lz4-java's LZ4BlockOutputStream, xerial's SnappyOutputStream), not in the
# standard lz4 or snappy frame formats.
LZ4_BLOCK_MAGIC = b"LZ4Block"
LZ4_BLOCK_HEADER = struct.Struct("<BII4x")  # token, compressed len, decompressed len, checksum
LZ4_BLOCK_RAW, LZ4_BLOCK_LZ4 = 0x10, 0x20
SNAPPY_MAGIC = b"\x82SNAPPY\x00"
SNAPPY_HEADER_LEN = len(SNAPPY_MAGIC) + 8  # magic, version int, compatible-version int

ROLLING_EVENTS_RE = re.compile(r"events_(\d+)_.+")


def codec_of(path: Path) -> str:
    """Compression codec named by a log file's suffix, or "none"."""
    return CODEC_SUFFIXES.get(path.suffix, "none")


def _decode_lz4_blocks(data: bytes) -> bytes:
    out, pos = [], 0
    while pos < len(data):
        if data[pos:pos + 8] != LZ4_BLOCK_MAGIC:
            raise ValueError(f"no LZ4Block header at byte {pos}")
        token, compressed_len, decompressed_len = LZ4_BLOCK_HEADER.unpack_from(data, pos + 8)
        pos += 8 + LZ4_BLOCK_HEADER.size
        block = data[pos:pos + compressed_len]
        pos += compressed_len
        method = token & 0xF0
        if method == LZ4_BLOCK_RAW:
            out.append(block)
        elif method == LZ4_BLOCK_LZ4:
            out.append(bytes(cramjam.lz4.decompress_block(block, output_len=decompressed_len)))
        else:
            raise ValueError(f"unknown LZ4Block compression method {method:#x}")
    return b"".join(out)


def _decode_snappy_blocks(data: bytes) -> bytes:
    if data[:len(SNAPPY_MAGIC)] != SNAPPY_MAGIC:
        raise ValueError("no SnappyOutputStream header")
    out, pos = [], SNAPPY_HEADER_LEN
    while pos < len(data):
        (length,) = struct.unpack_from(">i", data, pos)
        pos += 4
        out.append(bytes(cramjam.snappy.decompress_raw(data[pos:pos + length])))
        pos += length
    return b"".join(out)


def decompress(data: bytes, codec: str) -> bytes:
    if codec == "none":
        return data
    if codec == "zstd":
        return bytes(cramjam.zstd.decompress(data))
    if codec == "lz4":
        return _decode_lz4_blocks(data)
    if codec == "snappy":
        return _decode_snappy_blocks(data)
    raise ValueError(f"unknown codec {codec!r}")


def _count_event_lines(text: str, label: str) -> int:
    line_count = 0
    for line_no, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise InvalidEventLog(f"{label}:{line_no}: not valid JSON: {exc}") from exc
        if not isinstance(record, dict) or "Event" not in record:
            raise InvalidEventLog(f"{label}:{line_no}: missing required 'Event' field")
        line_count += 1
    return line_count


def _validate_log_file(path: Path) -> int:
    codec = codec_of(path)
    try:
        text = decompress(path.read_bytes(), codec).decode("utf-8")
    except (ValueError, OSError, cramjam.DecompressionError) as exc:
        raise InvalidEventLog(f"{path}: cannot read as a {codec} event log: {exc}") from exc
    return _count_event_lines(text, str(path))


def _rolling_event_files(directory: Path) -> list[Path]:
    indexed = []
    for child in directory.iterdir():
        match = ROLLING_EVENTS_RE.fullmatch(child.name)
        if match and child.is_file():
            indexed.append((int(match.group(1)), child))
    indexed.sort()
    if not indexed:
        raise InvalidEventLog(f"{directory}: no events_N_* files in rolling event log")
    if [i for i, _ in indexed] != list(range(1, len(indexed) + 1)):
        raise InvalidEventLog(f"{directory}: event file indexes are not 1..{len(indexed)}")
    if not any(child.name.startswith("appstatus_") for child in directory.iterdir()):
        raise InvalidEventLog(f"{directory}: no appstatus_ marker in rolling event log")
    return [child for _, child in indexed]


def log_layout(path: Path) -> dict[str, str]:
    """How Spark stored a log: "rolling-dir" or "single-file", and its codec.
    Raises InvalidEventLog if a rolling directory mixes codecs."""
    if path.is_dir():
        codecs = {codec_of(f) for f in _rolling_event_files(path)}
        if len(codecs) != 1:
            raise InvalidEventLog(f"{path}: rolling event files mix codecs {sorted(codecs)}")
        return {"layout": "rolling-dir", "compression": codecs.pop()}
    return {"layout": "single-file", "compression": codec_of(path)}


def validate_event_log(path: Path) -> int:
    """Structural check of a plain, compressed or rolling-directory event log.
    Returns the event count (summed over a rolling directory's files)."""
    if path.is_dir():
        files = _rolling_event_files(path)
        log_layout(path)
        total = sum(_validate_log_file(f) for f in files)
    else:
        total = _validate_log_file(path)
    if total == 0:
        raise InvalidEventLog(f"{path}: no event lines found")
    return total
