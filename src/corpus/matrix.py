"""Baseline and pairwise scenario definitions for the generation matrix.

The seven pairwise rows are a Taguchi L8(2^7) orthogonal array over
(aqe, shuffle_partitions, dynamic_allocation, speculation, slow_host, skew,
caching): the minimal covering design guaranteeing every pairwise combination
of the seven axes appears at least once. The all-baseline 8th row is dropped
since baseline_runs() already covers it. Do not add an 8th axis without
regenerating this whole table: L8 only covers 7 two-level factors, so an
eighth would silently stop being pairwise-covered.

Each row also carries the detector tags it is meant to make fire, so a
regenerated corpus can be checked for signal rather than just for having run:
scripts/verify_detectors.py records the tags that actually fire next to these.
"""
from __future__ import annotations

from dataclasses import dataclass, field

TABLE_FORMATS = ("parquet", "delta", "iceberg")

BASELINE_CONFIG = {
    "aqe": True,
    "shuffle_partitions": 200,
    "dynamic_allocation": True,
    "speculation": False,
    "slow_host": False,
    "skew": "none",
    "caching": "none",
}


@dataclass(frozen=True)
class Run:
    id: str
    spark_version: str
    table_format: str
    scenario: str
    config: dict
    targets_detectors: list[str] = field(default_factory=list)


def baseline_runs(versions: list[str]) -> list[Run]:
    return [
        Run(
            id=f"spark-{version}-{fmt}-baseline",
            spark_version=version,
            table_format=fmt,
            scenario="baseline",
            config=dict(BASELINE_CONFIG),
        )
        for version in versions
        for fmt in TABLE_FORMATS
    ]


@dataclass(frozen=True)
class ScenarioTemplate:
    id: str
    config: dict
    targets_detectors: list[str]


PAIRWISE_SCENARIOS: list[ScenarioTemplate] = [
    ScenarioTemplate(
        id="pairwise-01",
        config={
            "aqe": True, "shuffle_partitions": 200, "dynamic_allocation": True,
            "speculation": True, "slow_host": True, "skew": "injected",
            "caching": "memory-only",
        },
        targets_detectors=["SPEC", "HOST", "STRAG", "SKEW", "CACHE", "CSTOR"],
    ),
    ScenarioTemplate(
        id="pairwise-02",
        config={
            "aqe": True, "shuffle_partitions": 2000, "dynamic_allocation": False,
            "speculation": False, "slow_host": False, "skew": "injected",
            "caching": "memory-only",
        },
        targets_detectors=["PART", "TINY", "UTIL", "COLD", "SKEW", "CACHE", "CSTOR"],
    ),
    ScenarioTemplate(
        id="pairwise-03",
        config={
            "aqe": True, "shuffle_partitions": 2000, "dynamic_allocation": False,
            "speculation": True, "slow_host": True, "skew": "none",
            "caching": "none",
        },
        targets_detectors=["PART", "TINY", "UTIL", "COLD", "SPEC", "HOST", "STRAG"],
    ),
    ScenarioTemplate(
        id="pairwise-04",
        config={
            "aqe": False, "shuffle_partitions": 200, "dynamic_allocation": False,
            "speculation": False, "slow_host": True, "skew": "none",
            "caching": "memory-only",
        },
        targets_detectors=["SHFL", "SPILL", "UTIL", "COLD", "HOST", "STRAG", "CACHE", "CSTOR"],
    ),
    ScenarioTemplate(
        id="pairwise-05",
        config={
            "aqe": False, "shuffle_partitions": 200, "dynamic_allocation": False,
            "speculation": True, "slow_host": False, "skew": "injected",
            "caching": "none",
        },
        targets_detectors=["SHFL", "SPILL", "UTIL", "COLD", "SPEC", "SKEW"],
    ),
    ScenarioTemplate(
        id="pairwise-06",
        config={
            "aqe": False, "shuffle_partitions": 2000, "dynamic_allocation": True,
            "speculation": False, "slow_host": True, "skew": "injected",
            "caching": "none",
        },
        targets_detectors=["SHFL", "SPILL", "PART", "TINY", "HOST", "STRAG", "SKEW"],
    ),
    ScenarioTemplate(
        id="pairwise-07",
        config={
            "aqe": False, "shuffle_partitions": 2000, "dynamic_allocation": True,
            "speculation": True, "slow_host": False, "skew": "none",
            "caching": "memory-only",
        },
        targets_detectors=["SHFL", "SPILL", "PART", "TINY", "SPEC", "CACHE", "CSTOR"],
    ),
]


def pairwise_runs(latest_version: str) -> list[Run]:
    return [
        Run(
            id=t.id,
            spark_version=latest_version,
            table_format="parquet",
            scenario=t.id,
            config=t.config,
            targets_detectors=t.targets_detectors,
        )
        for t in PAIRWISE_SCENARIOS
    ]


# Standalone runs, outside the L8 array above: each is the baseline config
# plus one "failure" mode that makes the workload fail on purpose, so the
# failure detectors see logs a real cluster produced. The failures are keyed
# on partition index and task attempt number, never on timing; see
# workload/generate_events.py. A config without a "failure" key runs the
# normal workload.
FAILURE_SCENARIOS: list[ScenarioTemplate] = [
    # Every task fails its first attempt and succeeds on the retry.
    ScenarioTemplate(
        id="failure-task-retry",
        config={**BASELINE_CONFIG, "failure": "task-retry"},
        targets_detectors=["RETRY"],
    ),
    # A few tasks of one stage fail on every attempt, aborting the stage.
    ScenarioTemplate(
        id="failure-stage-abort",
        config={**BASELINE_CONFIG, "failure": "stage-abort"},
        targets_detectors=["FAIL", "SFAIL"],
    ),
    # One job of three fails; the application carries on and ends cleanly.
    ScenarioTemplate(
        id="failure-job",
        config={**BASELINE_CONFIG, "failure": "job-failure"},
        targets_detectors=["JOBS"],
    ),
    # The driver JVM halts mid-run, so the log has no application-end event.
    ScenarioTemplate(
        id="failure-killed-run",
        config={**BASELINE_CONFIG, "failure": "killed"},
        targets_detectors=["INCMP"],
    ),
]


def failure_runs(latest_version: str) -> list[Run]:
    return [
        Run(
            id=t.id,
            spark_version=latest_version,
            table_format="parquet",
            scenario=t.id,
            config=t.config,
            targets_detectors=t.targets_detectors,
        )
        for t in FAILURE_SCENARIOS
    ]


# Standalone runs, outside the L8 array above: the baseline config with the
# fact table persisted, read again by a second action, and run under too
# little storage memory to hold it, so caching partly fails. storage_pressure
# also turns on block-update logging (see src/corpus/orchestration.py): the
# cache detector reads what was stored where from SparkListenerBlockUpdated
# events, which Spark only writes to the event log when asked to.
CACHE_SCENARIOS: list[ScenarioTemplate] = [
    # Partitions that do not fit are dropped, so only some stay cached.
    ScenarioTemplate(
        id="cache-memory-only",
        config={
            **BASELINE_CONFIG, "caching": "memory-only",
            "second_action": "reread", "storage_pressure": True,
        },
        targets_detectors=["CSTOR"],
    ),
    # Partitions that do not fit in memory are written to disk instead.
    ScenarioTemplate(
        id="cache-memory-and-disk",
        config={
            **BASELINE_CONFIG, "caching": "memory-and-disk",
            "second_action": "reread", "storage_pressure": True,
        },
        targets_detectors=["CSTOR"],
    ),
]


def cache_runs(latest_version: str) -> list[Run]:
    return [
        Run(
            id=t.id,
            spark_version=latest_version,
            table_format="parquet",
            scenario=t.id,
            config=t.config,
            targets_detectors=t.targets_detectors,
        )
        for t in CACHE_SCENARIOS
    ]
