"""PizzaFlow dataset profiling and referential-integrity checks."""

import json
import sys
from typing import Any

import pandas as pd

import config as cfg
from load_and_join import read_csv


EXPECTED_COLUMNS = {
    "orders": {"order_id", "date", "time"},
    "order_details": {"order_details_id", "order_id", "pizza_id", "quantity"},
    "pizzas": {"pizza_id", "pizza_type_id", "size", "price"},
    "pizza_types": {"pizza_type_id", "name", "category", "ingredients"},
}


def profile_one(name: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = cfg.path_for(name)
    df = read_csv(name)

    candidate_pks = [
        c for c in df.columns
        if df[c].notna().all() and df[c].is_unique
    ]

    constant_cols = [
        c for c in df.columns
        if df[c].nunique(dropna=True) <= 1
    ]

    missing_expected = sorted(EXPECTED_COLUMNS[name] - set(df.columns))

    prof = {
        "file": path.name,
        "role": cfg.FILES[name]["role"],
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "column_names": df.columns.tolist(),
        "size_kb": round(path.stat().st_size / 1024, 1),
        "candidate_primary_keys": candidate_pks,
        "constant_columns": constant_cols,
        "missing_expected_columns": missing_expected,
        "null_counts": {c: int(df[c].isna().sum()) for c in df.columns},
        "distinct_counts": {
            c: int(df[c].nunique(dropna=True)) for c in df.columns
        },
    }
    return df, prof


def check_integrity(frames: dict[str, pd.DataFrame]) -> dict:
    orders = frames["orders"]
    details = frames["order_details"]
    pizzas = frames["pizzas"]
    types = frames["pizza_types"]

    checks = {
        "orders.order_id unique": bool(orders["order_id"].is_unique),
        "order_details.order_details_id unique": bool(
            details["order_details_id"].is_unique
        ),
        "order_details.order_id -> orders.order_id": bool(
            details["order_id"].isin(set(orders["order_id"])).all()
        ),
        "order_details.pizza_id -> pizzas.pizza_id": bool(
            details["pizza_id"].isin(set(pizzas["pizza_id"])).all()
        ),
        "pizzas.pizza_type_id -> pizza_types.pizza_type_id": bool(
            pizzas["pizza_type_id"].isin(set(types["pizza_type_id"])).all()
        ),
    }

    orphans = {
        "order_details_without_order": int(
            (~details["order_id"].isin(set(orders["order_id"]))).sum()
        ),
        "order_details_without_pizza": int(
            (~details["pizza_id"].isin(set(pizzas["pizza_id"]))).sum()
        ),
        "pizzas_without_type": int(
            (~pizzas["pizza_type_id"].isin(set(types["pizza_type_id"]))).sum()
        ),
    }

    return {
        "foreign_keys_resolve": checks,
        "orphan_counts": orphans,
    }


def check_relationships(frames: dict[str, pd.DataFrame]) -> dict:
    orders = frames["orders"]
    details = frames["order_details"]
    pizzas = frames["pizzas"]
    types = frames["pizza_types"]

    order_counts = details["order_id"].value_counts()
    type_counts = pizzas["pizza_type_id"].value_counts()

    return {
        "orders_to_order_details": {
            "relationship": "orders 1..* order_details",
            "met": bool(not details["order_id"].is_unique),
            "children_min": int(order_counts.min()),
            "children_median": float(order_counts.median()),
            "children_max": int(order_counts.max()),
        },
        "pizza_types_to_pizzas": {
            "relationship": "pizza_types 1..* pizzas",
            "met": bool(not pizzas["pizza_type_id"].is_unique),
            "children_min": int(type_counts.min()),
            "children_median": float(type_counts.median()),
            "children_max": int(type_counts.max()),
        },
        "pizza_types_to_categories": {
            "distinct_categories": int(types["category"].nunique()),
            "categories": sorted(types["category"].dropna().unique().tolist()),
        },
        "orders": {
            "rows": int(len(orders)),
            "distinct_order_ids": int(orders["order_id"].nunique()),
        },
    }


def check_eligibility(
    frames: dict[str, pd.DataFrame],
    prof: dict,
) -> dict:
    details = frames["order_details"]
    event_rows = len(details)

    return {
        "condition_1_related_tables": {
            "met": len(prof) >= 3,
            "tables": list(prof.keys()),
        },
        "condition_2_one_to_many": {
            "met": (
                not details["order_id"].is_unique
                and not frames["pizzas"]["pizza_type_id"].is_unique
            ),
            "associations": [
                "orders 1..* order_details",
                "pizza_types 1..* pizzas",
            ],
        },
        "condition_3_timestamp": {
            "met": True,
            "field": "orders.date + orders.time",
            "min": str(
                pd.to_datetime(
                    frames["orders"]["date"].astype(str)
                    + " "
                    + frames["orders"]["time"].astype(str),
                    dayfirst=True,
                ).min()
            ),
            "max": str(
                pd.to_datetime(
                    frames["orders"]["date"].astype(str)
                    + " "
                    + frames["orders"]["time"].astype(str),
                    dayfirst=True,
                ).max()
            ),
        },
        "condition_4_volume": {
            "met": event_rows >= cfg.MIN_EVENT_ROWS,
            "event_rows": int(event_rows),
            "required_rows": cfg.MIN_EVENT_ROWS,
            "note": (
                "The supplied dataset has 48,620 order-detail rows, "
                "so it does not meet a strict 50,000-event requirement."
                if event_rows < cfg.MIN_EVENT_ROWS
                else "Event-row threshold met."
            ),
        },
    }


def main() -> int:
    cfg.banner("SESSION 1 - PIZZAFLOW FILE PROFILING")

    frames, prof = {}, {}
    for name in cfg.FILES:
        df, p = profile_one(name)
        frames[name] = df
        prof[name] = p

        print(
            f"\nFILE: {p['file']:<22} "
            f"role={p['role']:<14} "
            f"rows={p['rows']:>6,} "
            f"cols={p['columns']}"
        )
        print(f" columns: {', '.join(p['column_names'])}")
        print(f" candidate primary keys: {p['candidate_primary_keys']}")

        if p["missing_expected_columns"]:
            print(f" ERROR missing expected columns: {p['missing_expected_columns']}")

    cfg.banner("REFERENTIAL INTEGRITY")
    integrity = check_integrity(frames)
    for label, ok in integrity["foreign_keys_resolve"].items():
        print(f" {'PASS' if ok else 'FAIL'} {label}")
    print(f" orphan records: {integrity['orphan_counts']}")

    cfg.banner("RELATIONSHIPS")
    relationships = check_relationships(frames)
    for name, result in relationships.items():
        print(f"{name}: {result}")

    cfg.banner("DATASET ELIGIBILITY")
    eligibility = check_eligibility(frames, prof)
    for key, result in eligibility.items():
        print(f" {'MET' if result['met'] else 'NOT MET'} {key}")

    report = {
        "profiles": prof,
        "integrity": integrity,
        "relationships": relationships,
        "eligibility": eligibility,
    }

    cfg.OUT_PROFILE.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )
    print(f"\nWrote {cfg.OUT_PROFILE}")

    all_fk = all(integrity["foreign_keys_resolve"].values())
    if not all_fk:
        return 1

    if not eligibility["condition_4_volume"]["met"]:
        print(
            "\nWARNING: dataset is below the previous 50,000-event "
            "eligibility threshold. The code will still run, but do not "
            "claim that the threshold is met in the report."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
