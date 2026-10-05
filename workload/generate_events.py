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

--payload-columns, --fact-source parquet and --second-action count exist for
the pairwise scenarios, which scale the fact table until the detectors they
target clear their floors: the extra columns add shuffle bytes, and reading
the fact table back from Parquet and counting it after the join gives the
caching detector a file-scan relation that two SQL executions read.

--dml replaces the join workload with Delta DML (MERGE, UPDATE, DELETE) over a
seeded table, for the Delta scenarios in src/corpus/matrix.py; it needs
--table-format delta.

--failure replaces the join workload with a small job that fails on purpose,
for the failure scenarios in src/corpus/matrix.py. Which task attempts fail is
decided by partition index and attempt number alone, so every run fails the
same way.
"""
from __future__ import annotations

import argparse
import threading
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
    parser.add_argument("--payload-columns", type=int, default=0)
    parser.add_argument("--fact-source", choices=["range", "parquet"], default="range")
    parser.add_argument("--second-action", choices=["none", "reread", "count"], default="none")
    parser.add_argument("--table-format", choices=["parquet", "delta", "iceberg"], default="parquet")
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--failure", choices=["none", *FAILURE_SCENARIOS], default="none")
    parser.add_argument("--dml", choices=["none", *DML_SCENARIOS], default="none")
    args = parser.parse_args()
    if args.dml != "none" and args.table_format != "delta":
        parser.error("--dml needs --table-format delta")
    return args


def build_fact_df(
    spark: SparkSession, row_count: int, skew: str, key_cardinality: int, payload_columns: int
):
    df = spark.range(row_count).withColumnRenamed("id", "row_id")
    if skew == "injected":
        # 80% of rows collapse onto one hot key, so a single join task holds
        # most of the shuffle. Spread over several keys, the hot rows land in
        # several partitions and no one partition or task stands out.
        df = df.withColumn(
            "join_key",
            fn.when(fn.rand() < 0.8, fn.lit(0).cast("long"))
            .otherwise((fn.rand() * key_cardinality).cast("long")),
        )
    else:
        df = df.withColumn("join_key", (fn.rand() * key_cardinality).cast("long"))
    # Random doubles do not compress, so each column adds about 8 bytes per
    # row to every shuffle of the fact table.
    for i in range(payload_columns):
        df = df.withColumn(f"payload_{i}", fn.rand())
    return df


def stage_as_parquet(spark: SparkSession, df, path: str):
    """Writes df to Parquet and returns it read back, so later plans scan a
    file relation instead of recomputing spark.range."""
    df.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


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


# Delta DML scenarios. Each seeds a Delta table partitioned by bucket, runs its
# statements against it, and checks the table ended up as the statements say,
# so a scenario that silently did nothing fails the run instead of shipping a
# log with no DML in it.
DML_BUCKETS = 4
# Every MATCH_EVERY-th seeded id is updated by a merge; the same number of new
# ids again, over a tenth of the seeded count, are inserted.
MATCH_EVERY = 5
INSERT_FRACTION = 10


def seed_delta_table(spark: SparkSession, row_count: int, path: str) -> None:
    (
        spark.range(row_count)
        .select(
            "id",
            (fn.col("id") % DML_BUCKETS).alias("bucket"),
            fn.rand().alias("value"),
            fn.lit("seed").alias("status"),
        )
        .write.format("delta").partitionBy("bucket").save(path)
    )


def build_merge_source(spark: SparkSession, row_count: int):
    """Rows for a merge: every MATCH_EVERY-th seeded id (matched, updated) and
    row_count // INSERT_FRACTION ids past the seeded range (not matched,
    inserted). Same columns as the target, so UPDATE SET * / INSERT * apply."""
    inserts = row_count // INSERT_FRACTION
    return (
        spark.range(row_count + inserts)
        .filter((fn.col("id") % MATCH_EVERY == 0) | (fn.col("id") >= row_count))
        .select(
            "id",
            (fn.col("id") % DML_BUCKETS).alias("bucket"),
            (fn.rand() + 1000).alias("value"),
            fn.lit("merged").alias("status"),
        )
    )


def count_where(spark: SparkSession, path: str, condition: str = "true") -> int:
    return spark.read.format("delta").load(path).where(condition).count()


def expect_count(actual: int, expected: int, what: str) -> None:
    if actual != expected:
        raise RuntimeError(f"{what}: expected {expected} rows, found {actual}")


def multiples_below(limit: int, step: int) -> int:
    """How many ids in range(limit) are divisible by step."""
    return -(-limit // step)


def run_merge_sql(spark: SparkSession, row_count: int, path: str) -> None:
    build_merge_source(spark, row_count).createOrReplaceTempView("merge_source")
    spark.sql(
        f"MERGE INTO delta.`{path}` AS t USING merge_source AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    inserted = row_count // INSERT_FRACTION
    expect_count(count_where(spark, path), row_count + inserted, "merge-sql total")
    expect_count(count_where(spark, path, "status = 'merged'"), multiples_below(row_count, MATCH_EVERY) + inserted, "merge-sql merged")


def run_merge_api(spark: SparkSession, row_count: int, path: str) -> None:
    from delta.tables import DeltaTable

    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(build_merge_source(spark, row_count).alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    inserted = row_count // INSERT_FRACTION
    expect_count(count_where(spark, path), row_count + inserted, "merge-api total")
    expect_count(count_where(spark, path, "status = 'merged'"), multiples_below(row_count, MATCH_EVERY) + inserted, "merge-api merged")


def run_update(spark: SparkSession, row_count: int, path: str) -> None:
    spark.sql(f"UPDATE delta.`{path}` SET value = value * 1.1, status = 'updated' WHERE id % 10 = 0")
    expect_count(count_where(spark, path), row_count, "update total")
    expect_count(count_where(spark, path, "status = 'updated'"), multiples_below(row_count, 10), "update updated")


def run_delete(spark: SparkSession, row_count: int, path: str) -> None:
    from delta.tables import DeltaTable

    DeltaTable.forPath(spark, path).delete("id % 4 = 0")
    expect_count(count_where(spark, path), row_count - multiples_below(row_count, 4), "delete total")


def run_concurrent_merges(spark: SparkSession, row_count: int, path: str) -> None:
    """Two threads of one session each merge one bucket into the same table at
    the same time. Each ON clause pins its own partition, so the transactions
    touch disjoint files and both commit; a merge without that predicate would
    conflict with the other under Delta's optimistic concurrency."""
    from delta.tables import DeltaTable

    source = build_merge_source(spark, row_count)
    buckets = [0, 1]
    start = threading.Barrier(len(buckets))
    errors: list[BaseException] = []

    def merge_bucket(bucket: int) -> None:
        try:
            spark.sparkContext.setJobDescription(f"merge bucket {bucket}")
            start.wait()
            (
                DeltaTable.forPath(spark, path).alias("t")
                .merge(
                    source.where(f"bucket = {bucket}").alias("s"),
                    f"t.bucket = {bucket} AND t.id = s.id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except BaseException as exc:  # re-raised on the main thread below
            errors.append(exc)

    threads = [threading.Thread(target=merge_bucket, args=(b,)) for b in buckets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError(f"concurrent merge failed: {errors[0]!r}") from errors[0]
    merged = " OR ".join(f"bucket = {b}" for b in buckets)
    expected = sum(
        1 for i in range(row_count + row_count // INSERT_FRACTION)
        if (i % MATCH_EVERY == 0 or i >= row_count) and i % DML_BUCKETS in buckets
    )
    expect_count(count_where(spark, path, f"status = 'merged' AND ({merged})"), expected, "concurrent merges")


DML_SCENARIOS = {
    "merge-sql": run_merge_sql,
    "merge-api": run_merge_api,
    "update": run_update,
    "delete": run_delete,
    "concurrent-merge": run_concurrent_merges,
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

    if args.dml != "none":
        table_path = f"{args.output_path}-delta-table"
        seed_delta_table(spark, args.row_count, table_path)
        DML_SCENARIOS[args.dml](spark, args.row_count, table_path)
        spark.stop()
        return

    fact_df = build_fact_df(
        spark, args.row_count, args.skew, args.join_cardinality, args.payload_columns
    )
    if args.fact_source == "parquet":
        fact_df = stage_as_parquet(spark, fact_df, f"{args.output_path}-fact-source")
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
    elif args.second_action == "count":
        # A second SQL execution over the same fact table, with no shuffle of
        # its own, so it adds a job without adding a 2000-task stage to the log.
        fact_df.count()
    spark.stop()


if __name__ == "__main__":
    main()
