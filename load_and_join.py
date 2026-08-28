"""Load, validate, and integrate the PizzaFlow dataset."""

import sys
import time
from pathlib import Path

import pandas as pd

import config as cfg


def read_csv(name: str) -> pd.DataFrame:
    """Read a PizzaFlow input file."""
    return pd.read_csv(cfg.path_for(name), encoding="cp1252" if name == "pizza_types" else "utf-8")


def load_frames() -> dict[str, pd.DataFrame]:
    """Read all four computational CSVs."""
    return {name: read_csv(name) for name in cfg.FILES}


def build_working_dataset(
    frames: dict[str, pd.DataFrame] | None = None,
    verbose: bool = True,
) -> tuple[pd.DataFrame, dict]:
    """
    Build the order-detail-level analytical dataset.

    Grain:
        one row per order_details_id.

    Joins:
        order_details -> orders       (many-to-one)
        order_details -> pizzas       (many-to-one)
        pizzas        -> pizza_types  (many-to-one)

    The order-detail row count must remain 48,620 because all three joins
    enrich an existing event/detail row rather than creating additional
    order-detail records.
    """
    if frames is None:
        frames = load_frames()

    orders = frames["orders"].copy()
    details = frames["order_details"].copy()
    pizzas = frames["pizzas"].copy()
    types = frames["pizza_types"].copy()

    rows_before = len(details)
    start = time.perf_counter()

    orders["order_ts"] = pd.to_datetime(
        orders["date"].astype(str) + " " + orders["time"].astype(str),
        dayfirst=True,
        errors="raise",
    )

    working = (
        details
        .merge(
            orders[["order_id", "order_ts"]],
            on="order_id",
            how="inner",
            validate="many_to_one",
        )
        .merge(
            pizzas[["pizza_id", "pizza_type_id", "size", "price"]],
            on="pizza_id",
            how="inner",
            validate="many_to_one",
        )
        .merge(
            types[["pizza_type_id", "name", "category"]],
            on="pizza_type_id",
            how="inner",
            validate="many_to_one",
        )
    )

    working["quantity"] = pd.to_numeric(working["quantity"], errors="raise")
    working["price"] = pd.to_numeric(working["price"], errors="raise")
    working["gross_revenue"] = working["quantity"] * working["price"]

    rows_after = len(working)

    report = {
        "grain": "one row per order_details_id",
        "rows_before": rows_before,
        "rows_after": rows_after,
        "delta": rows_after - rows_before,
        "distinct_orders": int(working["order_id"].nunique()),
        "distinct_pizzas": int(working["pizza_id"].nunique()),
        "distinct_pizza_types": int(working["pizza_type_id"].nunique()),
        "distinct_categories": int(working["category"].nunique()),
        "join_seconds": round(time.perf_counter() - start, 4),
        "join_path": "order_details |> orders |> pizzas |> pizza_types",
    }

    if verbose:
        cfg.banner("PIZZAFLOW JOIN PATH RECONCILIATION")
        print(f" grain             : {report['grain']}")
        print(f" join path         : {report['join_path']}")
        print(f" detail rows before: {rows_before:,}")
        print(f" detail rows after : {rows_after:,}")
        print(f" difference        : {report['delta']}")
        print(f" distinct orders   : {report['distinct_orders']:,}")
        print(f" categories        : {report['distinct_categories']}")
        print(f" join time         : {report['join_seconds']} s")

    assert rows_after == rows_before, (
        f"Order-detail row count changed: {rows_before} -> {rows_after}. "
        "Check primary/foreign-key uniqueness and orphan records."
    )

    return working, report


def main() -> int:
    cfg.banner("SESSION 1 - PIZZAFLOW LOAD AND JOIN")

    frames = load_frames()
    for name, df in frames.items():
        print(f" loaded {name:<18} {len(df):>6,} rows")

    working, _ = build_working_dataset(frames)
    working.to_parquet(cfg.OUT_JOINED, index=False)

    print(f"\nWrote {cfg.OUT_JOINED} ({len(working):,} rows)")

    counts = working[cfg.PARTITION_KEY].value_counts()
    cfg.banner(f"CATEGORY DISTRIBUTION - {cfg.PARTITION_KEY}")
    print(f" distinct categories : {counts.size}")
    print(
        f" records/category    : "
        f"min={counts.min():,} "
        f"median={int(counts.median()):,} "
        f"max={counts.max():,}"
    )
    print(f" skew ratio          : {counts.max() / counts.min():.2f} : 1")
    print(f"\n{counts.to_string()}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
