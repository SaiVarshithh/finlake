"""
Verify the Bronze stock-price Iceberg table.

Prints row counts, duplicate `(ticker, trade_date)` keys, and per-ticker date
ranges so backfills can be checked after the Airflow DAG runs.
"""

from __future__ import annotations

from pyspark.sql.functions import col, count, countDistinct, max, min

from spark.handler import build_spark
from spark.model.models import RawStockPrices


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    table = RawStockPrices.TABLE_NAME
    df = spark.table(table)

    total_rows = df.count()
    distinct_keys = df.select("ticker", "trade_date").distinct().count()
    duplicate_keys = total_rows - distinct_keys

    print("=" * 80)
    print(f"Table              : {table}")
    print(f"Total rows         : {total_rows}")
    print(f"Distinct keys      : {distinct_keys}")
    print(f"Duplicate keys     : {duplicate_keys}")
    print("=" * 80)

    if duplicate_keys:
        print("[Duplicate ticker/trade_date keys]")
        (
            df.groupBy("ticker", "trade_date")
            .agg(count("*").alias("records"))
            .where(col("records") > 1)
            .orderBy("ticker", "trade_date")
            .show(100, truncate=False)
        )
        raise RuntimeError("raw_stock_prices contains duplicate ticker/trade_date keys.")

    print("[Rows per ticker]")
    (
        df.groupBy("ticker")
        .agg(
            count("*").alias("rows"),
            countDistinct("trade_date").alias("trading_days"),
            min("trade_date").alias("first_trade_date"),
            max("trade_date").alias("last_trade_date"),
        )
        .orderBy("ticker")
        .show(100, truncate=False)
    )

    print("[Rows per trade_date]")
    df.groupBy("trade_date").count().orderBy("trade_date").show(370, truncate=False)

    spark.stop()
    print("[verify_bronze_stock_prices] Verification complete.")


if __name__ == "__main__":
    main()
