# spark-event-corpus

Generates real Spark event logs across Spark versions, table formats, and a
bounded scenario matrix, and sources additional real logs from public
sources. See `docs/superpowers/specs/2026-09-05-spark-log-corpus-design.md`
in the sparkforensics repo for the full design.

## Setup

    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt

## Run the tests

    pytest

## Generate the corpus

    python3 scripts/run_generation.py
    python3 scripts/fetch_external.py

Both require Docker and network access, and are meant to be run by hand, not
in CI.
