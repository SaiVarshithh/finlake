"""
FinLake Iceberg bootstrap job.

Creates a deterministic 100,000-row transaction dataset and writes it to an
Iceberg table backed by Nessie metadata and MinIO object storage.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    date_add,
    expr,
    format_string,
    lit,
    pmod,
    rand,
    round as spark_round,
    sha2,
    to_date,
    when,
)


CATALOG = os.getenv("ICEBERG_CATALOG", "nessie")
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "finlake_bronze")
TABLE = os.getenv("ICEBERG_TABLE", "transactions_test")
RECORD_COUNT = int(os.getenv("TEST_RECORD_COUNT", "100000"))
TABLE_IDENTIFIER = f"{CATALOG}.{NAMESPACE}.{TABLE}"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("finlake-iceberg-transactions-ingest")
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
        .config(
            f"spark.sql.catalog.{CATALOG}.authentication.type",
            os.getenv("NESSIE_AUTH_TYPE", "NONE"),
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.warehouse",
            os.getenv("ICEBERG_WAREHOUSE", "s3://finlake-warehouse/warehouse"),
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.s3.endpoint",
            os.getenv("S3_ENDPOINT", "http://finlake-minio:9000"),
        )
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false")
    )
    return builder.getOrCreate()


def array_lookup(values: list[str], index_column: str = "record_id") -> str:
    quoted_values = ",".join([f"'{value}'" for value in values])
    return (
        f"element_at(array({quoted_values}), "
        f"cast({index_column} % {len(values)} as int) + 1)"
    )


def make_transactions(spark: SparkSession):
    df = spark.range(0, RECORD_COUNT, 1, numPartitions=16).withColumnRenamed(
        "id", "record_id"
    )

    return (
        df.withColumn("transaction_id", format_string("TX%012d", col("record_id")))
        .withColumn("account_id", format_string("ACCT%08d", pmod(col("record_id"), lit(25000))))
        .withColumn("customer_id", format_string("CUST%07d", pmod(col("record_id"), lit(15000))))
        .withColumn("event_date", date_add(to_date(lit("2026-07-01")), pmod(col("record_id"), lit(31)).cast("int")))
        .withColumn(
            "event_ts",
            expr(
                "timestampadd(SECOND, cast(record_id % 86400 as int), "
                "cast(event_date as timestamp))"
            ),
        )
        .withColumn(
            "amount",
            spark_round((rand(42) * lit(9500.0)) + lit(1.0), 2).cast("decimal(12,2)"),
        )
        .withColumn("currency", lit("USD"))
        .withColumn(
            "merchant_category",
            expr(
                array_lookup(
                    [
                        "grocery",
                        "fuel",
                        "travel",
                        "online_services",
                        "electronics",
                        "cash_withdrawal",
                        "gaming",
                        "utilities",
                    ]
                )
            ),
        )
        .withColumn("country", expr(array_lookup(["US", "IN", "GB", "DE", "SG"])))
        .withColumn("channel", expr(array_lookup(["pos", "mobile", "web", "atm"])))
        .withColumn("is_card_present", pmod(col("record_id"), lit(3)) != 0)
        .withColumn(
            "risk_score",
            spark_round(
                when(col("merchant_category").isin("gaming", "cash_withdrawal"), lit(0.42))
                .otherwise(lit(0.12))
                + (rand(99) * lit(0.35)),
                4,
            ),
        )
        .withColumn(
            "ingestion_batch_id",
            lit(os.getenv("AIRFLOW_RUN_ID", datetime.now(timezone.utc).isoformat())),
        )
        .withColumn("payload_hash", sha2(concat(col("transaction_id"), col("customer_id")), 256))
        .withColumn("created_at", current_timestamp())
        .withColumn("processing_date", col("event_date"))
        .select(
            "transaction_id",
            "account_id",
            "customer_id",
            "event_ts",
            "event_date",
            "amount",
            "currency",
            "merchant_category",
            "country",
            "channel",
            "is_card_present",
            "risk_score",
            "ingestion_batch_id",
            "payload_hash",
            "created_at",
            "processing_date",
        )
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("FinLake Iceberg transaction ingest")
    print(f"Spark version       : {spark.version}")
    print(f"Target table        : {TABLE_IDENTIFIER}")
    print(f"Requested rows      : {RECORD_COUNT}")
    print(f"Nessie URI          : {spark.conf.get(f'spark.sql.catalog.{CATALOG}.uri')}")
    print(f"Iceberg warehouse   : {spark.conf.get(f'spark.sql.catalog.{CATALOG}.warehouse')}")
    print("=" * 80)

    transactions = make_transactions(spark)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_IDENTIFIER} (
            transaction_id STRING,
            account_id STRING,
            customer_id STRING,
            event_ts TIMESTAMP,
            event_date DATE,
            amount DECIMAL(12,2),
            currency STRING,
            merchant_category STRING,
            country STRING,
            channel STRING,
            is_card_present BOOLEAN,
            risk_score DOUBLE,
            ingestion_batch_id STRING,
            payload_hash STRING,
            created_at TIMESTAMP,
            processing_date DATE
        )
        USING iceberg
        PARTITIONED BY (processing_date)
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='zstd',
            'write.distribution-mode'='hash'
        )
        """
    )

    transactions.writeTo(TABLE_IDENTIFIER).option("check-ordering", "false").overwritePartitions()

    final_count = spark.table(TABLE_IDENTIFIER).count()
    print(f"Rows now visible in {TABLE_IDENTIFIER}: {final_count}")
    if final_count < RECORD_COUNT:
        raise RuntimeError(
            f"Expected at least {RECORD_COUNT} rows in {TABLE_IDENTIFIER}, found {final_count}"
        )

    spark.sql(
        f"""
        SELECT merchant_category, count(*) AS records
        FROM {TABLE_IDENTIFIER}
        GROUP BY merchant_category
        ORDER BY records DESC
        """
    ).show(20, truncate=False)

    spark.stop()
    print("[finlake] Iceberg ingest completed successfully.")


if __name__ == "__main__":
    main()
