"""
FinLake — quick Iceberg read-back verification.

Reads the transactions_test table from Nessie/MinIO and prints:
  - Row count
  - Schema
  - Partition summary (records per processing_date)
  - 5 sample rows

Run via kubectl (see Makefile or manually):
  kubectl run finlake-verify \
    --image=ghcr.io/saivarshithh/finlake-spark:latest \
    --restart=Never -n finlake \
    --env SPARK_MASTER=local[*] \
    --env SPARK_JOB_FILE=/opt/spark-jobs/verify_iceberg_job.py
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

CATALOG = os.getenv("ICEBERG_CATALOG", "nessie")
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "finlake_bronze")
TABLE = os.getenv("ICEBERG_TABLE", "transactions_test")
TABLE_IDENTIFIER = f"{CATALOG}.{NAMESPACE}.{TABLE}"
READ_METHOD = os.getenv("READ_METHOD", "nessie").lower()  # 'nessie' or 'minio'


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("finlake-iceberg-verify")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{CATALOG}.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.uri",
            os.getenv("NESSIE_URI", "http://finlake-nessie:19120/api/v1"),
        )
        .config(f"spark.sql.catalog.{CATALOG}.ref", os.getenv("NESSIE_REF", "main"))
        .config(f"spark.sql.catalog.{CATALOG}.authentication.type", "NONE")
        .config(
            f"spark.sql.catalog.{CATALOG}.warehouse",
            os.getenv("ICEBERG_WAREHOUSE", "s3://finlake-warehouse/warehouse"),
        )
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(
            f"spark.sql.catalog.{CATALOG}.s3.endpoint",
            os.getenv("S3_ENDPOINT", "http://finlake-minio:9000"),
        )
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false")
        .config(
            f"spark.sql.catalog.{CATALOG}.client.region",
            os.getenv("AWS_REGION", "us-east-1"),
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.s3.access-key-id",
            os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.s3.secret-access-key",
            os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        # Setting defaultCatalog enables Iceberg to resolve path-based reads (s3://...)
        # using this catalog's S3FileIO configuration.
        .config("spark.sql.defaultCatalog", CATALOG)
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 70

    # ── 1. Load Table based on READ_METHOD ────────────────────────────────────
    if READ_METHOD == "minio":
        warehouse_path = os.getenv("ICEBERG_WAREHOUSE", "s3://finlake-warehouse/warehouse")
        table_path = f"{warehouse_path}/{NAMESPACE}/{TABLE}"
        print(f"\nReading table directly from MinIO path: {table_path}")
        df = spark.read.format("iceberg").load(table_path)
    else:
        print(f"\nReading table from Nessie catalog: {TABLE_IDENTIFIER}")
        df = spark.table(TABLE_IDENTIFIER)

    total = df.count()
    print(sep)
    if READ_METHOD == "minio":
        print(f"  Path           : {table_path}")
    else:
        print(f"  Table          : {TABLE_IDENTIFIER}")
    print(f"  Total rows     : {total:,}")
    print(sep)

    # ── 2. Schema ─────────────────────────────────────────────────────────────
    print("\n[Schema]")
    df.printSchema()

    # ── 3. Partition summary ──────────────────────────────────────────────────
    print("[Rows per processing_date (partition)]")
    df.groupBy("processing_date").count().orderBy("processing_date").show(50, truncate=False)

    # ── 4. Merchant category breakdown ───────────────────────────────────────
    print("[Rows per merchant_category]")
    df.groupBy("merchant_category").count().orderBy("count", ascending=False).show(
        truncate=False
    )

    # ── 5. Sample rows ────────────────────────────────────────────────────────
    print("[5 sample rows]")
    df.select(
        "transaction_id", "account_id", "event_date", "amount", "merchant_category", "channel"
    ).show(5, truncate=False)

    # ── 6. Iceberg metadata (only accessible via catalog name) ────────────────
    print("[Iceberg snapshot history]")
    spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {TABLE_IDENTIFIER}.snapshots").show(
        truncate=False
    )

    if READ_METHOD == "minio":
        print(f"\n✅ Verification complete — {total:,} rows confirmed via MinIO direct path")
    else:
        print(f"\n✅ Verification complete — {total:,} rows confirmed in {TABLE_IDENTIFIER}")
    spark.stop()


if __name__ == "__main__":
    main()
