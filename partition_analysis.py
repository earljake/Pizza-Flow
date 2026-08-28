"""Analyze category-level and physical Spark partition balance."""

import csv
import sys

import config as cfg
from load_and_join import build_working_dataset
from parallel_compute import build_joined


def key_level(working) -> dict:
    counts = working[cfg.PARTITION_KEY].value_counts()

    return {
        "distinct_keys": int(counts.size),
        "min": int(counts.min()),
        "median": int(counts.median()),
        "max": int(counts.max()),
        "skew_ratio": round(
            counts.max() / counts.min(),
            2,
        ),
        "heaviest": counts.head(5).to_dict(),
        "lightest": counts.tail(5).to_dict(),
        "counts": counts,
    }


def partition_level(joined, partitions: int) -> dict:
    sizes = (
        joined
        .repartition(partitions, cfg.PARTITION_KEY)
        .rdd
        .glom()
        .map(len)
        .collect()
    )

    total = sum(sizes)
    even = total / partitions if partitions else 0

    min_size = min(sizes) if sizes else 0
    max_size = max(sizes) if sizes else 0

    skew_ratio = (
        round(max_size / min_size, 2)
        if min_size > 0
        else float("inf")
    )

    worst_vs_even = (
        round(max_size / even, 2)
        if even > 0
        else 0.0
    )

    return {
        "partitions": partitions,
        "sizes": sizes,
        "min": min_size,
        "max": max_size,
        "even_share": round(even, 1),
        "skew_ratio": skew_ratio,
        "worst_vs_even": worst_vs_even,
    }


def main() -> int:
    cfg.banner("SESSION 1 - PIZZAFLOW PARTITION ANALYSIS")

    working, _ = build_working_dataset(verbose=False)
    kl = key_level(working)

    cfg.banner(f"KEY LEVEL - {cfg.PARTITION_KEY}")
    print(f"Distinct categories : {kl['distinct_keys']}")
    print(
        f"Records/category    : "
        f"min={kl['min']:,} "
        f"median={kl['median']:,} "
        f"max={kl['max']:,}"
    )
    print(f"Skew ratio          : {kl['skew_ratio']}:1")
    print("\nAll category counts:")
    for key, value in kl["counts"].items():
        print(f"  {key:<12} {value:>8,}")

    spark = cfg.build_spark()
    rows = []

    try:
        joined, _ = build_joined(
            spark,
            verbose=False,
        )

        cfg.banner("PHYSICAL SPARK PARTITIONS")

        for partitions in cfg.PARTITION_SETTINGS:
            result = partition_level(
                joined,
                partitions,
            )

            print(
                f"{partitions} partitions: "
                f"min={result['min']:,} "
                f"max={result['max']:,} "
                f"skew={result['skew_ratio']}:1 "
                f"worst/even={result['worst_vs_even']}x"
            )

            for index, size in enumerate(result["sizes"]):
                rows.append(
                    {
                        "level": "spark_partition",
                        "setting": partitions,
                        "identifier": index,
                        "record_count": int(size),
                        "even_share": result["even_share"],
                        "vs_even": (
                            round(size / result["even_share"], 3)
                            if result["even_share"] > 0
                            else 0.0
                        ),
                    }
                )

    finally:
        spark.stop()

    key_even = len(working) / kl["distinct_keys"]

    for key, count in kl["counts"].items():
        rows.append(
            {
                "level": "partition_key",
                "setting": cfg.PARTITION_KEY,
                "identifier": key,
                "record_count": int(count),
                "even_share": round(key_even, 1),
                "vs_even": (
                    round(count / key_even, 3)
                    if key_even > 0
                    else 0.0
                ),
            }
        )

    fields = [
        "level",
        "setting",
        "identifier",
        "record_count",
        "even_share",
        "vs_even",
    ]

    with open(
        cfg.OUT_PARTITIONS,
        "w",
        newline="",
        encoding="utf-8",
    ) as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {cfg.OUT_PARTITIONS} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
