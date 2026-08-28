"""
Shared configuration for MIT 261 Session 1 - PizzaFlow.

PizzaFlow:
A Transactional, Order-Driven and Aggregated Revenue Intelligence
Platform for Category-Based Monetization Models in Pizzeria Retail.

The supplied dataset contains five CSV files:
    orders.csv
    order_details.csv
    pizzas.csv
    pizza_types.csv
    data_dictionary.csv

The first four are operational/analytical tables. data_dictionary.csv is
documentation and is not part of the computational join.

Important dataset fact:
The supplied data contains 48,620 order-detail rows and 21,350 orders.
This is below the 50,000-event threshold used by the previous CMA-Flow
eligibility script. This configuration therefore reports that fact rather
than falsely claiming that the dataset satisfies a 50,000-row requirement.

No separate monetization-rule table exists in the supplied dataset. The
computational workload therefore uses menu-price revenue:
    gross_revenue = quantity * price

Category comes from pizza_types.category and is the business-aligned
partition/aggregation key for PizzaFlow.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SESSION_DIR = REPO_ROOT
DATA_DIR = SESSION_DIR / "Datasets"
RESULTS_DIR = SESSION_DIR / "results"
DOCS_DIR = REPO_ROOT / "docs"
ARCH_DIR = REPO_ROOT / "architecture"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)
ARCH_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "orders": {"file": "orders.csv", "role": "Event Header"},
    "order_details": {"file": "order_details.csv", "role": "Event Detail"},
    "pizzas": {"file": "pizzas.csv", "role": "Entity"},
    "pizza_types": {"file": "pizza_types.csv", "role": "Entity"},
}

DICTIONARY_FILE = DATA_DIR / "data_dictionary.csv"

# Business/workload fields
PARTITION_KEY = "category"
METRIC_FIELD = "gross_revenue"
EVENT_TIME_FIELD = "order_ts"

ORDER_KEY = "order_id"
ORDER_DETAIL_KEY = "order_details_id"
PIZZA_KEY = "pizza_id"
PIZZA_TYPE_KEY = "pizza_type_id"
QUANTITY_FIELD = "quantity"
PRICE_FIELD = "price"

# The supplied dataset has 48,620 order-detail events.
MIN_EVENT_ROWS = 50_000

# Bounded parallelism conditions.
PARTITION_SETTINGS = (2, 4, 8)
BENCHMARK_REPEATS = 3
BASELINE_REPEATS = 5
CHOSEN_PARTITIONS = 4

TOLERANCE = 1e-6

# Spark settings
SPARK_APP_NAME = "MIT261-PizzaFlow-Session1"
SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "2g"
SPARK_SHUFFLE_PARTITIONS = "8"

# Small dimension tables suitable for broadcast joins.
BROADCAST_FILES = ("pizzas", "pizza_types")

# Outputs
OUT_PROFILE = RESULTS_DIR / "file_profile.json"
OUT_JOINED = RESULTS_DIR / "working_dataset.parquet"
OUT_BASELINE = RESULTS_DIR / "baseline_result.csv"
OUT_BENCHMARK = RESULTS_DIR / "session1_benchmark.csv"
OUT_PARTITIONS = RESULTS_DIR / "partition_sizes.csv"
OUT_FINAL = RESULTS_DIR / "category_revenue.parquet"
OUT_VALIDATION = RESULTS_DIR / "validation_report.json"


def path_for(name: str) -> Path:
    return DATA_DIR / FILES[name]["file"]


def build_spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)
