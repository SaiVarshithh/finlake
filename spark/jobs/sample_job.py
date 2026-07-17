"""
FinLake — Sample PySpark Job
============================
A minimal but complete PySpark batch job to validate the container setup.

Run locally (inside the container):
    spark-submit /opt/spark/jobs/sample_job.py

Run via Docker:
    docker run --rm \
      -e SPARK_JOB_FILE=/opt/spark/jobs/sample_job.py \
      ghcr.io/saivarshithh/finlake-spark:latest
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("finlake-sample-job")
        # Enable Delta Lake (uncomment if delta-spark is available)
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  FinLake PySpark — Sample Job")
    print(f"  Spark version : {spark.version}")
    print("=" * 60)

    # ── Create a tiny sample DataFrame ────────────────────────────────────────
    data = [
        ("trade_001", "BTC/USD", 65_432.10, 1.5, "buy"),
        ("trade_002", "ETH/USD",  3_210.55, 5.0, "sell"),
        ("trade_003", "SOL/USD",    175.88, 20.0, "buy"),
    ]
    schema = ["trade_id", "symbol", "price", "quantity", "side"]

    df = spark.createDataFrame(data, schema)

    # ── Basic transformation ───────────────────────────────────────────────────
    df_enriched = (
        df
        .withColumn("notional", col("price") * col("quantity"))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source", lit("finlake-sample"))
    )

    print("\n>>> Sample trade data:")
    df_enriched.show(truncate=False)

    # ── Aggregation ───────────────────────────────────────────────────────────
    df_agg = (
        df_enriched
        .groupBy("side")
        .agg({"notional": "sum", "trade_id": "count"})
        .withColumnRenamed("sum(notional)", "total_notional")
        .withColumnRenamed("count(trade_id)", "trade_count")
    )

    print("\n>>> Aggregation by side:")
    df_agg.show()

    spark.stop()
    print("\n[finlake] Job completed successfully.")


if __name__ == "__main__":
    main()
