"""Lightweight structural check that a file is a well-formed Spark NDJSON
event log: every non-blank line is valid JSON with an 'Event' field. This is
deliberately not a full parse against sparkforensics' own TypeScript parser:
that would pull a JS toolchain into a Python repo for a check this simple
substitutes for.
"""
from __future__ import annotations

import json
from pathlib import Path


class InvalidEventLog(ValueError):
    pass


def validate_ndjson_event_log(path: Path) -> int:
    line_count = 0
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
    if line_count == 0:
        raise InvalidEventLog(f"{path}: no event lines found")
    return line_count
