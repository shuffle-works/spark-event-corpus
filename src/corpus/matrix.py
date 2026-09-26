"""Baseline and pairwise scenario definitions for the generation matrix.

The seven pairwise rows are a Taguchi L8(2^7) orthogonal array over
(aqe, shuffle_partitions, dynamic_allocation, speculation, slow_host, skew,
caching): the minimal covering design guaranteeing every pairwise combination
of the seven axes appears at least once. The all-baseline 8th row is dropped
since baseline_runs() already covers it. Do not add an 8th axis without
regenerating this whole table: L8 only covers 7 two-level factors, so an
eighth would silently stop being pairwise-covered.

Each row also carries the detector tags it makes fire, so a regenerated
corpus can be checked for signal rather than just for having run:
scripts/verify_detectors.py records the tags that actually fire next to these.
Tags a row was designed for but does not reach are kept as known_misses.
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
    known_misses: dict[str, str] = field(default_factory=dict)


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
    """targets_detectors lists the tags the scenario makes fire on its
    committed log. known_misses maps each tag the scenario was designed for
    but does not reach to why, with the measured distance from the floor, so
    a miss is recorded instead of hidden or tuned away."""

    id: str
    config: dict
    targets_detectors: list[str]
    known_misses: dict[str, str] = field(default_factory=dict)


# Scale knobs every pairwise row shares, on top of its L8 axes. The baseline
# workload is too small for most detector floors, so each pairwise row runs
# on a third worker (HOST needs at least 3 hosts or executors), splits its
# map stages into 20 tasks (HOST also needs a stage of 15 or more), and, by
# default, runs 10M rows with 8 extra columns of random doubles, which gives
# the join about 680 MiB of shuffle (SHFL needs 50 MiB; PART a 256 MiB
# partition). Rows that need more override row_count or payload_columns.
PAIRWISE_SCALE = {
    "third_worker": True,
    "row_count": 10_000_000,
    "payload_columns": 8,
}
PAIRWISE_SPARK_CONFS = {"spark.default.parallelism": "20"}

# Per-knob confs, each for the rows whose targets need it.
# SPEC counts losing speculative attempts only once they add up to 60 s;
# starting speculation at 12 s makes every loser at least that old.
SPECULATION_CONFS = {"spark.speculation.minTaskRuntime": "12s"}
# About 7 MB of execution memory per task, so the join spills (SPILL).
SPILL_CONFS = {"spark.memory.fraction": "0.02"}
# Without block updates the log does not say where cached partitions went,
# and CSTOR has nothing to measure on Spark 2.3 or later.
BLOCK_UPDATE_CONFS = {"spark.eventLog.logBlockUpdates.enabled": "true"}

# COLD needs the first executor to arrive more than 30 s after the first
# stage; the workers sleep this long before starting (see compose.yaml).
COLD_START_DELAY_S = 45

# The caching rows read the fact table back from Parquet and count it again
# after the join: CACHE counts file-scan relations read by two or more SQL
# executions, and a spark.range source is not a file scan.
CACHE_REUSE = {"fact_source": "parquet", "second_action": "count"}

# Why the caching rows miss CSTOR, which flags an RDD with some but under 90%
# of its partitions cached. Caching here is all or nothing: without a memory
# cap the fact table fits whole, and under spark.memory.fraction=0.02 not one
# partition does, so there is no partial cache to flag.
CSTOR_FITS_WHOLE = (
    "all 20 partitions of the persisted fact table stay cached ({size}), per block "
    "updates; CSTOR needs some but under 90%"
)
CSTOR_FITS_NOTHING = (
    "none of the 20 partitions of the persisted fact table is cached: at "
    "spark.memory.fraction=0.02 each is larger than storage memory; CSTOR needs some "
    "but under 90%"
)


def pairwise_config(axes: dict, *confs: dict, **knobs) -> dict:
    """One pairwise row's config: its seven L8 axes, the shared scale knobs,
    any per-row overrides, and the merged spark_confs."""
    spark_confs = dict(PAIRWISE_SPARK_CONFS)
    for conf in confs:
        spark_confs.update(conf)
    return {**axes, **PAIRWISE_SCALE, **knobs, "spark_confs": spark_confs}


PAIRWISE_SCENARIOS: list[ScenarioTemplate] = [
    # 50M narrow rows, so the hot key's join task runs long enough to keep
    # the stage open while the slow host's speculated tail is killed.
    ScenarioTemplate(
        id="pairwise-01",
        config=pairwise_config(
            {
                "aqe": True, "shuffle_partitions": 200, "dynamic_allocation": True,
                "speculation": True, "slow_host": True, "skew": "injected",
                "caching": "memory-only",
            },
            SPECULATION_CONFS, BLOCK_UPDATE_CONFS,
            row_count=50_000_000, payload_columns=0, **CACHE_REUSE,
        ),
        targets_detectors=["SPEC", "HOST", "STRAG", "SKEW", "CACHE"],
        known_misses={"CSTOR": CSTOR_FITS_WHOLE.format(size="59.5 MB")},
    ),
    ScenarioTemplate(
        id="pairwise-02",
        config=pairwise_config(
            {
                "aqe": True, "shuffle_partitions": 2000, "dynamic_allocation": False,
                "speculation": False, "slow_host": False, "skew": "injected",
                "caching": "memory-only",
            },
            BLOCK_UPDATE_CONFS,
            worker_start_delay_s=COLD_START_DELAY_S, **CACHE_REUSE,
        ),
        targets_detectors=["PART", "TINY", "UTIL", "COLD", "CACHE"],
        known_misses={
            "SKEW": "join stage P95/median of 2.5 against a floor of 3 (2.60 to 2.88 in "
            "earlier runs); the one hot key is a single task of 2000, below P95",
            "CSTOR": CSTOR_FITS_WHOLE.format(size="653 MB"),
        },
    ),
    # No injected skew: 4 equally common join keys put all the data in 4 of
    # the 2000 partitions, 16M rows making each one about 270 MiB (PART).
    ScenarioTemplate(
        id="pairwise-03",
        config=pairwise_config(
            {
                "aqe": True, "shuffle_partitions": 2000, "dynamic_allocation": False,
                "speculation": True, "slow_host": True, "skew": "none",
                "caching": "none",
            },
            SPECULATION_CONFS,
            row_count=16_000_000, join_cardinality=4, worker_start_delay_s=COLD_START_DELAY_S,
        ),
        targets_detectors=["PART", "TINY", "UTIL", "COLD", "STRAG"],
        known_misses={
            "SPEC": "no speculative attempt launched in this log; in other runs the "
            "losers were the slow host's killed tail attempts, which were never written "
            "because the application ends with that stage",
            "HOST": "the slow host's executor completed no task: it registers late on "
            "0.3 CPU, and its few tail tasks are lost as above",
        },
    ),
    ScenarioTemplate(
        id="pairwise-04",
        config=pairwise_config(
            {
                "aqe": False, "shuffle_partitions": 200, "dynamic_allocation": False,
                "speculation": False, "slow_host": True, "skew": "none",
                "caching": "memory-only",
            },
            SPILL_CONFS, BLOCK_UPDATE_CONFS,
            worker_start_delay_s=COLD_START_DELAY_S, **CACHE_REUSE,
        ),
        targets_detectors=["SHFL", "SPILL", "UTIL", "COLD", "HOST", "STRAG", "CACHE"],
        known_misses={"CSTOR": CSTOR_FITS_NOTHING},
    ),
    ScenarioTemplate(
        id="pairwise-05",
        config=pairwise_config(
            {
                "aqe": False, "shuffle_partitions": 200, "dynamic_allocation": False,
                "speculation": True, "slow_host": False, "skew": "injected",
                "caching": "none",
            },
            SPECULATION_CONFS, SPILL_CONFS,
            worker_start_delay_s=COLD_START_DELAY_S,
        ),
        targets_detectors=["SHFL", "SPILL", "UTIL", "COLD"],
        known_misses={
            "SPEC": "one speculative attempt and no counted loser against a floor of 5 "
            "losers and 60 s; with no slow host the only slow task is the hot key's",
            "SKEW": "fires only from executor warm-up in the first map stage, never from "
            "the hot key (1 of 200 join tasks, below P95): P95/median 3.05 there in this "
            "log, fired in 1 of 3 runs",
        },
    ),
    ScenarioTemplate(
        id="pairwise-06",
        config=pairwise_config(
            {
                "aqe": False, "shuffle_partitions": 2000, "dynamic_allocation": True,
                "speculation": False, "slow_host": True, "skew": "injected",
                "caching": "none",
            },
            SPILL_CONFS,
        ),
        targets_detectors=["SHFL", "SPILL", "PART", "TINY", "HOST", "STRAG", "SKEW"],
    ),
    # Same 4-key shape as pairwise-03.
    ScenarioTemplate(
        id="pairwise-07",
        config=pairwise_config(
            {
                "aqe": False, "shuffle_partitions": 2000, "dynamic_allocation": True,
                "speculation": True, "slow_host": False, "skew": "none",
                "caching": "memory-only",
            },
            SPECULATION_CONFS, SPILL_CONFS, BLOCK_UPDATE_CONFS,
            row_count=16_000_000, join_cardinality=4, **CACHE_REUSE,
        ),
        targets_detectors=["SHFL", "SPILL", "PART", "TINY", "CACHE"],
        known_misses={
            "SPEC": "no speculative attempt launched, in this log or in two earlier "
            "runs: with no slow host and no skew, no task lags far enough behind the "
            "stage's median to be copied",
            "CSTOR": CSTOR_FITS_NOTHING,
        },
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
            known_misses=t.known_misses,
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
