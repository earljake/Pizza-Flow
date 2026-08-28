"""
PySpark parallel implementation for PizzaFlow.

The Spark workload uses the same order-detail grain and the same aggregation
as sequential_baseline.py. Small dimension tables are broadcast, then the
joined working dataset is explicitly repartitioned by category.
"""

import json
import sys
import time

import pandas as pd

import config as cfg


def load_spark_frames(spark):
    from pyspark.sql import functions as F

    # ------------------------------------------------------------------
    # CSV READER
    # ------------------------------------------------------------------
    #
    # For orders, inferSchema is disabled because Spark can incorrectly
    # infer the "time" column as a timestamp and attach the current date.
    #
    # Example of the bad behavior:
    #
    #     date = 01/01/2015
    #     time = 11:38:36
    #
    # can become:
    #
    #     01/01/2015 2026-08-22 11:38:36
    #
    # Therefore date/time are read as strings and parsed explicitly.
    # ------------------------------------------------------------------

    def read(name, infer_schema=True):
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", infer_schema)
            .csv(str(cfg.path_for(name)))
        )

    # ------------------------------------------------------------------
    # ORDERS
    # ------------------------------------------------------------------

    orders_raw = read(
        "orders",
        infer_schema=False,
    )

    # Your dataset uses:
    #
    #     DD/MM/YYYY
    #
    # and the time can contain either:
    #
    #     09:52:21
    #
    # or:
    #
    #     9:52:21
    #
    # Therefore the Spark pattern uses:
    #
    #     dd/MM/yyyy H:mm:ss
    #
    # "H" accepts both one-digit and two-digit hours.
    # ------------------------------------------------------------------

    orders = (
        orders_raw
        .withColumn(
            "order_ts",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.trim(F.col("date")),
                    F.trim(F.col("time")),
                ),
                "dd/MM/yyyy H:mm:ss",
            ),
        )
        .select(
            F.col("order_id").cast("long").alias("order_id"),
            "order_ts",
        )
    )

    # ------------------------------------------------------------------
    # OTHER TABLES
    # ------------------------------------------------------------------

    details = read("order_details")
    pizzas = read("pizzas")
    types = read("pizza_types")

    return orders, details, pizzas, types


def build_joined(spark, verbose: bool = True):
    from pyspark.sql import functions as F

    orders, details, pizzas, types = load_spark_frames(spark)

    # ------------------------------------------------------------------
    # ORDER-DETAIL GRAIN CHECK
    # ------------------------------------------------------------------

    rows_before = details.count()

    input_partitions = details.rdd.getNumPartitions()

    # ------------------------------------------------------------------
    # JOIN PATH
    #
    # order_details
    #       |
    #       +---- orders
    #       |
    #       +---- pizzas
    #                 |
    #                 +---- pizza_types
    #
    # Small dimension tables are broadcast.
    # ------------------------------------------------------------------

    joined = (
        details

        # --------------------------------------------------------------
        # order_details -> orders
        # --------------------------------------------------------------
        .join(
            F.broadcast(orders),
            on="order_id",
            how="inner",
        )

        # --------------------------------------------------------------
        # order_details -> pizzas
        # --------------------------------------------------------------
        .join(
            F.broadcast(pizzas),
            on="pizza_id",
            how="inner",
        )

        # --------------------------------------------------------------
        # pizzas -> pizza_types
        # --------------------------------------------------------------
        .join(
            F.broadcast(types),
            on="pizza_type_id",
            how="inner",
        )

        # --------------------------------------------------------------
        # Ensure numeric columns have the correct types.
        # --------------------------------------------------------------
        .withColumn(
            "quantity",
            F.col("quantity").cast("long"),
        )
        .withColumn(
            "price",
            F.col("price").cast("double"),
        )

        # --------------------------------------------------------------
        # Derived revenue metric.
        # --------------------------------------------------------------
        .withColumn(
            "gross_revenue",
            F.col("quantity") * F.col("price"),
        )
    )

    # ------------------------------------------------------------------
    # CACHE JOINED DATA
    #
    # The joined DataFrame is used multiple times later in the program,
    # so caching avoids unnecessarily repeating the joins.
    # ------------------------------------------------------------------

    joined.cache()

    rows_after = joined.count()

    # ------------------------------------------------------------------
    # INSPECT SPARK PHYSICAL PLAN
    # ------------------------------------------------------------------

    plan = (
        joined._jdf
        .queryExecution()
        .executedPlan()
        .toString()
    )

    report = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "delta": rows_after - rows_before,
        "input_partitions": input_partitions,
        "broadcast_hash_joins": plan.count(
            "BroadcastHashJoin"
        ),
        "sort_merge_joins": plan.count(
            "SortMergeJoin"
        ),
    }

    if verbose:
        cfg.banner("PIZZAFLOW SPARK JOIN")

        print(
            f"Rows before join  : "
            f"{rows_before:,}"
        )

        print(
            f"Rows after join   : "
            f"{rows_after:,}"
        )

        print(
            f"Difference        : "
            f"{report['delta']}"
        )

        print(
            f"Input partitions  : "
            f"{input_partitions}"
        )

        print(
            f"Broadcast joins   : "
            f"{report['broadcast_hash_joins']}"
        )

        print(
            f"Sort-merge joins  : "
            f"{report['sort_merge_joins']}"
        )

    # ------------------------------------------------------------------
    # REFERENTIAL-INTEGRITY / GRAIN VALIDATION
    # ------------------------------------------------------------------

    assert rows_after == rows_before, (
        "Spark join changed the order-detail row count. "
        "Check key uniqueness and referential integrity."
    )

    return joined, report


def compute_parallel(joined, partitions: int):
    from pyspark.sql import functions as F

    if partitions < 1:
        raise ValueError(
            "partitions must be >= 1"
        )

    # ------------------------------------------------------------------
    # REPARTITION BY CATEGORY
    #
    # This is the explicit partitioning step for the parallel-compute
    # experiment.
    # ------------------------------------------------------------------

    partitioned = joined.repartition(
        partitions,
        cfg.PARTITION_KEY,
    )

    # ------------------------------------------------------------------
    # CATEGORY AGGREGATION
    # ------------------------------------------------------------------

    result = (
        partitioned
        .groupBy(
            cfg.PARTITION_KEY
        )
        .agg(
            F.countDistinct(
                "order_id"
            ).alias(
                "order_count"
            ),

            F.count(
                "order_details_id"
            ).alias(
                "item_line_count"
            ),

            F.sum(
                "quantity"
            ).alias(
                "units_sold"
            ),

            F.sum(
                "gross_revenue"
            ).alias(
                "revenue_total"
            ),

            F.avg(
                "gross_revenue"
            ).alias(
                "revenue_mean"
            ),
        )
    )

    return partitioned, result


def validate(
    parallel_pd: pd.DataFrame,
    baseline_pd: pd.DataFrame,
    verbose: bool = True,
) -> dict:

    # ------------------------------------------------------------------
    # SORT BOTH RESULTS
    # ------------------------------------------------------------------

    parallel_pd = (
        parallel_pd
        .sort_values(
            cfg.PARTITION_KEY
        )
        .reset_index(
            drop=True
        )
    )

    baseline_pd = (
        baseline_pd
        .sort_values(
            cfg.PARTITION_KEY
        )
        .reset_index(
            drop=True
        )
    )

    # ------------------------------------------------------------------
    # GROUP COUNT CHECK
    # ------------------------------------------------------------------

    group_match = (
        len(parallel_pd)
        == len(baseline_pd)
    )

    # ------------------------------------------------------------------
    # CATEGORY KEY CHECK
    # ------------------------------------------------------------------

    merged = parallel_pd.merge(
        baseline_pd,
        on=cfg.PARTITION_KEY,
        suffixes=(
            "_par",
            "_base",
        ),
        how="outer",
        indicator=True,
    )

    keys_match = bool(
        (
            merged["_merge"]
            == "both"
        ).all()
    )

    # ------------------------------------------------------------------
    # METRICS TO COMPARE
    # ------------------------------------------------------------------

    metric_cols = [
        "order_count",
        "item_line_count",
        "units_sold",
        "revenue_total",
        "revenue_mean",
    ]

    differences = {}

    if (
        keys_match
        and len(merged) > 0
    ):
        for col in metric_cols:

            differences[col] = float(
                (
                    merged[
                        f"{col}_par"
                    ]
                    -
                    merged[
                        f"{col}_base"
                    ]
                )
                .abs()
                .max()
            )

    else:

        differences = {
            col: float("inf")
            for col in metric_cols
        }

    # ------------------------------------------------------------------
    # FINAL VALIDATION
    # ------------------------------------------------------------------

    passed = (
        group_match
        and keys_match
        and differences[
            "order_count"
        ] == 0

        and differences[
            "item_line_count"
        ] == 0

        and differences[
            "units_sold"
        ] == 0

        and differences[
            "revenue_total"
        ] < cfg.TOLERANCE

        and differences[
            "revenue_mean"
        ] < cfg.TOLERANCE
    )

    report = {
        "parallel_groups": int(
            len(parallel_pd)
        ),

        "baseline_groups": int(
            len(baseline_pd)
        ),

        "group_count_match": (
            group_match
        ),

        "partition_keys_match": (
            keys_match
        ),

        "max_differences": (
            differences
        ),

        "tolerance": (
            cfg.TOLERANCE
        ),

        "passed": bool(
            passed
        ),
    }

    if verbose:
        cfg.banner(
            "PIZZAFLOW CORRECTNESS VALIDATION"
        )

        print(
            json.dumps(
                report,
                indent=2,
            )
        )

    if not passed:
        raise AssertionError(
            "Parallel result does not "
            "match the sequential baseline."
        )

    return report


def main() -> int:
    from sequential_baseline import (
        run_baseline
    )

    cfg.banner(
        "SESSION 1 - PIZZAFLOW PARALLEL COMPUTE"
    )

    spark = cfg.build_spark()

    try:

        # ==============================================================
        # BUILD JOINED DATASET
        # ==============================================================

        joined, join_report = (
            build_joined(spark)
        )

        # ==============================================================
        # CATEGORY AGGREGATION
        # ==============================================================

        cfg.banner(
            f"CATEGORY AGGREGATION - "
            f"{cfg.CHOSEN_PARTITIONS} PARTITIONS"
        )

        start = time.perf_counter()

        partitioned, result = (
            compute_parallel(
                joined,
                cfg.CHOSEN_PARTITIONS,
            )
        )

        groups = result.count()

        elapsed = (
            time.perf_counter()
            - start
        )

        print(
            f"Configured partitions : "
            f"{partitioned.rdd.getNumPartitions()}"
        )

        print(
            f"Result groups         : "
            f"{groups}"
        )

        print(
            f"Execution time        : "
            f"{elapsed:.4f} s"
        )

        # ==============================================================
        # DISPLAY RESULTS
        # ==============================================================

        print(
            "\nCategory results:"
        )

        (
            result
            .orderBy(
                "revenue_total",
                ascending=False,
            )
            .show(
                truncate=False
            )
        )

        # ==============================================================
        # CONVERT SPARK RESULT TO PANDAS
        # ==============================================================

        parallel_pd = (
            result.toPandas()
        )

        # ==============================================================
        # LOAD SEQUENTIAL BASELINE
        # ==============================================================

        if cfg.OUT_BASELINE.exists():

            baseline_pd = (
                pd.read_csv(
                    cfg.OUT_BASELINE
                )
            )

        else:

            baseline_pd, _ = (
                run_baseline(
                    working=None,
                    verbose=False,
                )
            )

        # ==============================================================
        # VALIDATE PARALLEL RESULT
        # ==============================================================

        validation = validate(
            parallel_pd,
            baseline_pd,
        )

        # ==============================================================
        # FINAL OUTPUT
        # ==============================================================

        final = (
            parallel_pd
            .sort_values(
                cfg.PARTITION_KEY
            )
            .reset_index(
                drop=True
            )
        )

        final.to_parquet(
            cfg.OUT_FINAL,
            index=False,
        )

        print(
            f"\nWrote {cfg.OUT_FINAL} "
            f"({len(final)} rows)"
        )

        # ==============================================================
        # VALIDATION REPORT
        # ==============================================================

        validation_data = {
            "project": "PizzaFlow",

            "join": join_report,

            "aggregation": {
                "partition_key": (
                    cfg.PARTITION_KEY
                ),

                "partitions": (
                    cfg.CHOSEN_PARTITIONS
                ),

                "groups": groups,

                "seconds": round(
                    elapsed,
                    4,
                ),
            },

            "validation": validation,
        }

        cfg.OUT_VALIDATION.write_text(
            json.dumps(
                validation_data,
                indent=2,
            ),
            encoding="utf-8",
        )

        print(
            f"Wrote {cfg.OUT_VALIDATION}"
        )

    finally:

        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())