#!/usr/bin/env python3
"""Parameterized PySpark workload used for every baseline and scenario run.
Behavioral scenario axes (skew, persistence) are script parameters; AQE,
shuffle-partitions, dynamic-allocation, and speculation are pure Spark confs
set by the caller via spark-submit --conf and need no script-side handling.
Slow-host simulation is a Docker Compose worker CPU limit, not a script
parameter either; see compose.yaml.

--persist-mode memory-and-disk and --second-action reread exist for the cache
scenarios in src/corpus/matrix.py: the persisted fact table is read again by a
second action after the join, so the storage detectors see a cached RDD reused
across jobs.

--failure replaces the join workload with a small job that fails on purpose,
for the failure scenarios in src/corpus/matrix.py. Which task attempts fail is
decided by partition index and attempt number alone, so every run fails the
same way.
"""
from __future__ import annotations

import argparse
import time

from py4j.protocol import Py4JJavaError
from pyspark import SparkContext, StorageLevel, TaskContext
from pyspark.errors import PythonException
from pyspark.sql import SparkSession, functions as fn

# Pinned so the failure scenarios do not depend on Spark's default. The
# retry scenario needs at least 2; the stage-abort scenario fails this often.
TASK_MAX_FAILURES = 4
# How long a doomed task attempt runs before raising. RETRY only fires once
# retried attempts have wasted 30 s in total, and in the stage-abort scenario
# the healthy tasks must all have finished before the stage is aborted.
FAILED_ATTEMPT_SECONDS = 10
# Must match KILLED_RUN_EXIT_CODE in src/corpus/orchestration.py.
KILLED_RUN_EXIT_CODE = 137

PERSIST_LEVELS = {
    "memory-only": StorageLevel.MEMORY_ONLY,
    "memory-and-disk": StorageLevel.MEMORY_AND_DISK,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--row-count", type=int, default=5_000_000)
    parser.add_argument("--skew", choices=["none", "injected"], default="none")
    parser.add_argument("--join-tables", type=int, default=2)
    parser.add_argument("--join-cardinality", type=int, default=1000)
    parser.add_argument("--persist-mode", choices=["none", *PERSIST_LEVELS], default="none")
    parser.add_argument("--second-action", choices=["none", "reread"], default="none")
    parser.add_argument("--table-format", choices=["parquet", "delta", "iceberg"], default="parquet")
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--failure", choices=["none", *FAILURE_SCENARIOS], default="none")
    return parser.parse_args()


def build_fact_df(spark: SparkSession, row_count: int, skew: str, key_cardinality: int):
    df = spark.range(row_count).withColumnRenamed("id", "row_id")
    if skew == "injected":
        # 80% of rows collapse onto 1% of key values.
        hot_keys = max(1, key_cardinality // 100)
        df = df.withColumn(
            "join_key",
            fn.when(fn.rand() < 0.8, (fn.rand() * hot_keys).cast("long"))
            .otherwise((fn.rand() * key_cardinality).cast("long")),
        )
    else:
        df = df.withColumn("join_key", (fn.rand() * key_cardinality).cast("long"))
    return df


def build_dimension_df(spark: SparkSession, cardinality: int, suffix: str):
    return (
        spark.range(cardinality)
        .withColumnRenamed("id", "join_key")
        .withColumn(f"attr_{suffix}", fn.concat(fn.lit(f"val-{suffix}-"), fn.col("join_key").cast("string")))
    )


def write_output(df, table_format: str, output_path: str) -> None:
    if table_format == "iceberg":
        df.writeTo("local.default.events").using("iceberg").createOrReplace()
    else:
        df.write.format(table_format).mode("overwrite").save(output_path)


class InjectedTaskFailure(RuntimeError):
    pass


def fail_attempt(index: int) -> None:
    time.sleep(FAILED_ATTEMPT_SECONDS)
    raise InjectedTaskFailure(
        f"partition {index}, attempt {TaskContext.get().attemptNumber()}: injected failure"
    )


def expect_job_failure(action) -> None:
    """Runs an action that must fail with the injected failure, and swallows
    that failure so the application goes on to end cleanly. Any other outcome
    means the scenario did not do what it claims, which must not pass
    silently."""
    try:
        action()
    # Spark 4 surfaces a failed RDD job as PythonException, Spark 3.5 as
    # Py4JJavaError.
    except (PythonException, Py4JJavaError) as exc:
        if InjectedTaskFailure.__name__ not in str(exc):
            raise
        return
    raise RuntimeError("the injected failure did not fail the job")


def run_task_retry(sc: SparkContext) -> None:
    # 4 partitions each waste one FAILED_ATTEMPT_SECONDS attempt: 4 retried
    # attempts and 40 s of waste, over RETRY's floor of 3 attempts and 30 s.
    def fail_first_attempt(index, rows):
        if TaskContext.get().attemptNumber() == 0:
            fail_attempt(index)
        return rows

    sc.parallelize(range(400), 4).mapPartitionsWithIndex(fail_first_attempt).count()


def run_stage_abort(sc: SparkContext) -> None:
    # The doomed partitions are the highest-numbered, which the scheduler
    # launches last, so the 16 healthy tasks have all finished by the time a
    # doomed one exhausts its attempts. That leaves 4 failed tasks of 20 in
    # the aborted stage, over FAIL's floor of 10 tasks and a 5% failure rate.
    num_partitions, num_doomed = 20, 4

    def fail_doomed_partitions(index, rows):
        if index >= num_partitions - num_doomed:
            fail_attempt(index)
        return rows

    expect_job_failure(
        lambda: sc.parallelize(range(2000), num_partitions)
        .mapPartitionsWithIndex(fail_doomed_partitions)
        .count()
    )


def run_job_failure(sc: SparkContext) -> None:
    # One failed job out of three completed ones is a 33% job failure rate.
    def fail_every_partition(index, rows):
        fail_attempt(index)

    sc.parallelize(range(200), 2).count()
    expect_job_failure(
        lambda: sc.parallelize(range(200), 2).mapPartitionsWithIndex(fail_every_partition).count()
    )
    sc.parallelize(range(200), 2).count()


def run_killed(sc: SparkContext) -> None:
    sc.parallelize(range(200), 2).count()
    # The event log is flushed when the listener bus handles a job end, so
    # drain the bus first: the log then holds the application start and the
    # finished job, and lacks only the application end.
    sc._jsc.sc().listenerBus().waitUntilEmpty()
    # halt() skips the shutdown hooks, so SparkContext.stop() never runs and
    # no application-end event is written, exactly as for a killed driver.
    sc._jvm.java.lang.Runtime.getRuntime().halt(KILLED_RUN_EXIT_CODE)


FAILURE_SCENARIOS = {
    "task-retry": run_task_retry,
    "stage-abort": run_stage_abort,
    "job-failure": run_job_failure,
    "killed": run_killed,
}


def main() -> None:
    args = parse_args()
    builder = SparkSession.builder.appName("spark-event-corpus-workload")
    if args.failure != "none":
        builder = builder.config("spark.task.maxFailures", str(TASK_MAX_FAILURES))
    spark = builder.getOrCreate()

    if args.failure != "none":
        FAILURE_SCENARIOS[args.failure](spark.sparkContext)
        spark.stop()
        return

    fact_df = build_fact_df(spark, args.row_count, args.skew, args.join_cardinality)
    if args.persist_mode != "none":
        fact_df = fact_df.persist(PERSIST_LEVELS[args.persist_mode])

    result = fact_df
    for i in range(args.join_tables):
        dim_df = build_dimension_df(spark, args.join_cardinality, str(i))
        result = result.join(dim_df, on="join_key", how="inner")

    write_output(result, args.table_format, args.output_path)
    if args.second_action == "reread":
        # A second job over the persisted fact table: partitions that were
        # cached are read back, the rest are recomputed and offered to the
        # cache again.
        fact_df.groupBy("join_key").count().collect()
    spark.stop()


if __name__ == "__main__":
    main()
