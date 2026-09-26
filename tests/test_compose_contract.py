"""compose.yaml and env_for_run() are two halves of one contract: every ${VAR}
the compose file interpolates has to be a key env_for_run() produces, or the
run silently comes up misconfigured (an unset ${VAR} interpolates to an empty
string -- an empty bind-mount path, or a --conf with no value).

Also pins the handful of literal --conf flags that were each added to fix a
real, separately-diagnosed failure, so a future edit to the command block
can't quietly drop one.
"""
from pathlib import Path
import re

from corpus.matrix import Run
from corpus.orchestration import env_for_run

COMPOSE_PATH = Path(__file__).resolve().parent.parent / "compose.yaml"
COMPOSE_TEXT = COMPOSE_PATH.read_text()


def representative_run() -> Run:
    return Run(
        id="test", spark_version="3.5.3", table_format="parquet", scenario="baseline",
        config={
            "aqe": True, "shuffle_partitions": 200, "dynamic_allocation": True,
            "speculation": False, "slow_host": False, "skew": "none", "caching": "none",
        },
    )


def test_every_compose_variable_is_produced_by_env_for_run():
    referenced = set(re.findall(r"\$\{(\w+)\}", COMPOSE_TEXT))
    produced = set(env_for_run(representative_run(), Path("/tmp/x"), Path("/tmp/y")))
    assert referenced, "expected compose.yaml to interpolate at least one variable"
    assert referenced - produced == set()


def test_env_for_run_values_are_all_non_empty_strings():
    env = env_for_run(representative_run(), Path("/tmp/x"), Path("/tmp/y"))
    for key, value in env.items():
        assert isinstance(value, str), key
        # PACKAGES_FLAG/TABLE_FORMAT_CONF_FLAGS are legitimately empty for
        # parquet, and STORAGE_CONF_FLAGS outside the cache scenarios; every
        # other variable becoming empty would break the run.
        if key not in {"PACKAGES_FLAG", "TABLE_FORMAT_CONF_FLAGS", "STORAGE_CONF_FLAGS"}:
            assert value != "", key


def test_hard_won_conf_flags_are_still_present():
    # Comment-only mentions (e.g. this repo's own explanatory comments citing
    # a flag by name) must not satisfy this check: only a real --conf line
    # proves the flag itself, not just a note about it, is still there.
    command_lines = "\n".join(
        line for line in COMPOSE_TEXT.splitlines() if not line.lstrip().startswith("#")
    )
    for flag in (
        # Ivy's cache must point somewhere writable: the spark user has no HOME.
        "spark.jars.ivy=",
        # Spark 4.x rolling event logs produce a directory, not one NDJSON file.
        "spark.eventLog.rolling.enabled=false",
        # Compressed event logs are not the plain-text NDJSON the corpus ships.
        "spark.eventLog.compress=false",
        # Without this every join broadcasts and no shuffle is ever produced.
        "spark.sql.autoBroadcastJoinThreshold=-1",
        # Without this, AQE erases the real shuffle's partition count back
        # down to a handful on every aqe=true row.
        "spark.sql.adaptive.coalescePartitions.enabled=false",
        # Without this, dynamic allocation can reclaim an executor holding
        # shuffle data the real shuffle now produces.
        "spark.dynamicAllocation.shuffleTracking.enabled=true",
    ):
        assert flag in command_lines, flag


def test_both_workers_advertise_an_explicit_core_count():
    """The slow-host axis works by throttling worker-2's CPU quota while it
    still advertises the same task slots; left to auto-detect, the Worker JVM
    would derive its cores from that quota and just take fewer tasks instead
    of running them more slowly."""
    assert COMPOSE_TEXT.count('"--cores", "2"') == 2
