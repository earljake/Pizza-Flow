"""Benchmark sequential versus bounded-parallel PizzaFlow aggregation."""

import csv
import statistics
import sys
import time

import config as cfg
from load_and_join import build_working_dataset
from parallel_compute import build_joined, compute_parallel
from sequential_baseline import run_baseline


def bench_one(joined, partitions: int, repeats: int) -> dict:
    times = []
    groups = None

    for _ in range(repeats):
        start = time.perf_counter()
        _, result = compute_parallel(joined, partitions)
        groups = result.count()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "partitions": partitions,
        "runs": [round(t, 4) for t in times],
        "median": round(statistics.median(times), 4),
        "min": round(min(times), 4),
        "max": round(max(times), 4),
        "groups": groups,
    }


def main() -> int:
    cfg.banner("SESSION 1 - PIZZAFLOW BENCHMARK")

    working, join_report = build_working_dataset(verbose=False)
    _, baseline_report = run_baseline(
        working,
        verbose=False,
    )

    print(f"Joined detail rows : {len(working):,}")
    print(f"Baseline median    : {baseline_report['median_seconds']} s")
    print(f"Baseline groups    : {baseline_report['groups']}")

    spark = cfg.build_spark()

    try:
        joined, spark_join = build_joined(
            spark,
            verbose=False,
        )

        print(f"Spark joined rows  : {joined.count():,}")
        print(
            f"BroadcastHashJoin : "
            f"{spark_join['broadcast_hash_joins']}"
        )
        print(
            f"SortMergeJoin     : "
            f"{spark_join['sort_merge_joins']}"
        )

        results = []

        cfg.banner("BOUNDED PARALLELISM CONDITIONS")

        for partitions in cfg.PARTITION_SETTINGS:
            row = bench_one(
                joined,
                partitions,
                cfg.BENCHMARK_REPEATS,
            )

            row["baseline_median"] = baseline_report["median_seconds"]
            row["speedup_vs_baseline"] = (
                baseline_report["median_seconds"] / row["median"]
                if row["median"] > 0
                else 0.0
            )

            results.append(row)

            print(
                f"{partitions} partitions: "
                f"median={row['median']:.4f}s "
                f"speedup={row['speedup_vs_baseline']:.2f}x "
                f"groups={row['groups']}"
            )

        with open(
            cfg.OUT_BENCHMARK,
            "w",
            newline="",
            encoding="utf-8",
        ) as fh:
            fields = [
                "partitions",
                "runs",
                "median",
                "min",
                "max",
                "groups",
                "baseline_median",
                "speedup_vs_baseline",
            ]
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(results)

        print(f"\nWrote {cfg.OUT_BENCHMARK}")

    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
