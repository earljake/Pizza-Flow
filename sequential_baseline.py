"""Sequential Pandas reference workload for PizzaFlow."""

import statistics
import sys
import time

import pandas as pd

import config as cfg
from load_and_join import build_working_dataset


def compute(working: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate the order-detail-level working dataset by pizza category.

    Metrics:
        order_count       distinct orders represented in the category
        item_line_count   number of order-detail records
        units_sold        total quantity
        revenue_total     sum(quantity * price)
        revenue_mean      mean revenue per order-detail line
    """
    result = (
        working
        .groupby(cfg.PARTITION_KEY)
        .agg(
            order_count=("order_id", "nunique"),
            item_line_count=("order_details_id", "count"),
            units_sold=("quantity", "sum"),
            revenue_total=("gross_revenue", "sum"),
            revenue_mean=("gross_revenue", "mean"),
        )
        .reset_index()
    )

    return result


def run_baseline(
    working: pd.DataFrame | None = None,
    repeats: int = cfg.BASELINE_REPEATS,
    verbose: bool = True,
) -> tuple[pd.DataFrame, dict]:
    if working is None:
        working, _ = build_working_dataset(verbose=False)

    times = []
    result = None

    for _ in range(repeats):
        start = time.perf_counter()
        result = compute(working)
        times.append(time.perf_counter() - start)

    assert result is not None

    report = {
        "runs": [round(t, 4) for t in times],
        "median_seconds": round(statistics.median(times), 4),
        "mean_seconds": round(statistics.fmean(times), 4),
        "groups": int(len(result)),
        "repeats": repeats,
        "total_revenue": float(result["revenue_total"].sum()),
        "total_units": int(result["units_sold"].sum()),
    }

    if verbose:
        cfg.banner("PIZZAFLOW SEQUENTIAL BASELINE")
        for i, t in enumerate(times, 1):
            print(f"  run {i}: {t:.4f} s")
        print(f"\n  median : {report['median_seconds']:.4f} s")
        print(f"  mean   : {report['mean_seconds']:.4f} s")
        print(f"  groups : {report['groups']}")
        print(f"  revenue: ${report['total_revenue']:,.2f}")

    return result, report


def main() -> int:
    cfg.banner("SESSION 1 - PIZZAFLOW SEQUENTIAL BASELINE")

    working, _ = build_working_dataset()
    result, report = run_baseline(working)

    display = result.sort_values("revenue_total", ascending=False)

    print("\nCategory revenue:")
    print(display.to_string(index=False))

    print(
        f"\nTotal revenue across {len(result)} categories: "
        f"${result['revenue_total'].sum():,.2f}"
    )

    result.sort_values(cfg.PARTITION_KEY).to_csv(
        cfg.OUT_BASELINE,
        index=False,
    )
    print(f"\nWrote {cfg.OUT_BASELINE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
