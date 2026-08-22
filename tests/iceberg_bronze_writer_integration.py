"""Disposable local-catalog integration check for the Bronze Iceberg writer."""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import tempfile

from pyspark.sql import SparkSession


def main() -> None:
    warehouse = tempfile.mkdtemp(prefix="finlake-iceberg-test-")
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("finlake-bronze-writer-integration")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.testcat", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.testcat.type", "hadoop")
        .config("spark.sql.catalog.testcat.warehouse", warehouse)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.sql("CREATE NAMESPACE testcat.finlake_bronze")
        spark.sql(
            """
            CREATE TABLE testcat.finlake_bronze.raw_stock_prices (
                ticker STRING NOT NULL,
                exchange STRING NOT NULL,
                trade_date DATE NOT NULL,
                open DECIMAL(10, 2) NOT NULL,
                high DECIMAL(10, 2) NOT NULL,
                low DECIMAL(10, 2) NOT NULL,
                close DECIMAL(10, 2) NOT NULL,
                adj_close DECIMAL(10, 2),
                volume BIGINT NOT NULL,
                ingestion_ts TIMESTAMP NOT NULL,
                source_batch_id STRING NOT NULL
            ) USING iceberg
            PARTITIONED BY (days(trade_date), ticker)
            """
        )
        table = "testcat.finlake_bronze.raw_stock_prices"
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD months(trade_date)")
        spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD days(trade_date)")
        spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD ticker")
        spark.sql(
            f"""
            ALTER TABLE {table} SET TBLPROPERTIES (
                'write.distribution-mode'='hash',
                'write.spark.fanout.enabled'='false'
            )
            """
        )

        from spark.model.models import RawStockPrices

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        ticker_count = 105
        range_days = 800
        start = date(2024, 6, 15)
        rows = []
        for ticker_index in range(ticker_count):
            ticker = f"T{ticker_index:03d}"
            for day_offset in range(range_days):
                trade_date = start + timedelta(days=day_offset)
                price = Decimal("100.00") + Decimal(ticker_index)
                rows.append(
                    (
                        ticker,
                        "NSE",
                        trade_date,
                        price,
                        price + Decimal("1.00"),
                        price - Decimal("1.00"),
                        price,
                        price,
                        1000 + day_offset,
                        now,
                        "first",
                    )
                )
        writer = RawStockPrices.__new__(RawStockPrices)
        writer.spark = spark
        writer.write_raw_stock_prices_df(spark.createDataFrame(rows, RawStockPrices.get_schema()))

        updates = [
            ("T000", "NSE", start, Decimal("100.00"), Decimal("102.00"), Decimal("99.00"), Decimal("101.50"), Decimal("101.50"), 1500, now, "second"),
            ("T000", "NSE", start + timedelta(days=range_days), Decimal("101.00"), Decimal("102.00"), Decimal("100.00"), Decimal("101.50"), Decimal("101.50"), 1750, now, "second"),
        ]
        writer.write_raw_stock_prices_df(spark.createDataFrame(updates, RawStockPrices.get_schema()))

        result = spark.table(table)
        assert result.count() == (ticker_count * range_days) + 1
        updated = result.where(f"ticker = 'T000' AND trade_date = DATE '{start.isoformat()}'").first()
        assert updated.close == Decimal("101.50")
        assert updated.source_batch_id == "second"
        assert spark.sql(f"SHOW TBLPROPERTIES {table} ('write.spark.fanout.enabled')").first().value == "false"
        data_file_count = spark.table(f"{table}.files").count()
        assert data_file_count <= 30, f"Expected monthly files, found {data_file_count}"
        print(
            "Iceberg partition evolution and 105-ticker/800-day idempotent MERGE: "
            f"PASS ({data_file_count} data files)"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
