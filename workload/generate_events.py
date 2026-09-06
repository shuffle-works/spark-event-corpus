#!/usr/bin/env python3
"""Parameterized PySpark workload used for every baseline and scenario run.
Behavioral scenario axes (skew, persistence) are script parameters; AQE,
shuffle-partitions, dynamic-allocation, and speculation are pure Spark confs
set by the caller via spark-submit --conf and need no script-side handling.
Slow-host simulation is a Docker Compose worker CPU limit, not a script
parameter either; see compose.yaml.
"""
from __future__ import annotations

import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as fn


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--row-count", type=int, default=5_000_000)
    parser.add_argument("--skew", choices=["none", "injected"], default="none")
    parser.add_argument("--join-tables", type=int, default=2)
    parser.add_argument("--join-cardinality", type=int, default=1000)
    parser.add_argument("--persist-mode", choices=["none", "memory-only"], default="none")
    parser.add_argument("--table-format", choices=["parquet", "delta", "iceberg"], default="parquet")
    parser.add_argument("--output-path", required=True)
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


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("spark-event-corpus-workload").getOrCreate()

    fact_df = build_fact_df(spark, args.row_count, args.skew, args.join_cardinality)
    if args.persist_mode == "memory-only":
        fact_df = fact_df.persist(StorageLevel.MEMORY_ONLY)

    result = fact_df
    for i in range(args.join_tables):
        dim_df = build_dimension_df(spark, args.join_cardinality, str(i))
        result = result.join(dim_df, on="join_key", how="inner")

    write_output(result, args.table_format, args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()
