"""Choose and document the PizzaFlow partition strategy."""

import sys

import pandas as pd

import config as cfg
from load_and_join import build_working_dataset, load_frames


def candidate_columns(working: pd.DataFrame) -> list[str]:
    """Return categorical columns that are usable for partitioning."""
    candidates = []
    for col in working.columns:
        if col in {
            cfg.PARTITION_KEY,
            "category",
            "pizza_type_id",
            "pizza_id",
            "size",
            "order_id",
        }:
            if working[col].notna().all() and working[col].nunique() > 1:
                candidates.append(col)
    return candidates


def evaluate(working: pd.DataFrame) -> list[dict]:
    results = []

    for col in candidate_columns(working):
        counts = working[col].value_counts()
        skew = (
            float(counts.max() / counts.min())
            if counts.min() > 0
            else float("inf")
        )

        results.append(
            {
                "column": col,
                "distinct": int(counts.size),
                "min": int(counts.min()),
                "median": float(counts.median()),
                "max": int(counts.max()),
                "skew_ratio": round(skew, 2),
                "business_alignment": (
                    col == cfg.PARTITION_KEY
                ),
            }
        )

    return results


def main() -> int:
    cfg.banner("SESSION 1 - PIZZAFLOW PARTITION STRATEGY")

    frames = load_frames()
    working, join_report = build_working_dataset(
        frames,
        verbose=False,
    )

    print(
        f"Joined dataset: {len(working):,} rows x "
        f"{len(working.columns)} columns"
    )

    cfg.banner("CANDIDATE PARTITION KEYS")

    candidates = evaluate(working)

    header = (
        f" {'column':<18}"
        f"{'distinct':>10}"
        f"{'min':>8}"
        f"{'median':>10}"
        f"{'max':>8}"
        f"{'skew':>8}"
        f" {'business'}"
    )
    print(header)
    print("-" * len(header))

    for c in candidates:
        print(
            f" {c['column']:<18}"
            f"{c['distinct']:>10,}"
            f"{c['min']:>8,}"
            f"{c['median']:>10,.1f}"
            f"{c['max']:>8,}"
            f"{c['skew_ratio']:>8.2f}"
            f" {'YES' if c['business_alignment'] else 'no'}"
        )

    chosen = next(
        (c for c in candidates if c["column"] == cfg.PARTITION_KEY),
        None,
    )

    if chosen is None:
        print(f"\nERROR: '{cfg.PARTITION_KEY}' is not available.")
        return 1

    cfg.banner("PARTITION DECISION")
    print(f"Chosen key       : {cfg.PARTITION_KEY}")
    print(f"Distinct values  : {chosen['distinct']}")
    print(f"Skew ratio       : {chosen['skew_ratio']}:1")
    print(
        "Reason           : category is the central PizzaFlow business "
        "dimension for category-based revenue analysis and is present "
        "after all required joins."
    )

    print(
        "\nNote: pizza_id has much higher cardinality/skew in the supplied "
        "data, while category has only four business groups and a much "
        "more balanced distribution."
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
