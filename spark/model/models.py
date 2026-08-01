from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.app_config import get_app_config
from spark.handler import SparkHandler

app_config = get_app_config()


class RawStockPrices(SparkHandler):
    TABLE_NAME = f"{app_config.catalog}.finlake_bronze.raw_stock_prices"
    PARTITION_BY = ["days(trade_date)", "ticker"]
    PARTITION_EVOLUTION = ["days(trade_date)", "ticker"]
    TABLE_PROPERTIES = {
        "format-version": "2",
        "write.parquet.compression-codec": "snappy",
        # hash: Spark pre-shuffles by partition key before writing.
        # Each task gets data for fewer unique partitions → fewer simultaneous compressors.
        # 'none' was WRONG — it forces FanoutDataWriter with ALL partitions open at once.
        "write.distribution-mode": "hash",
    }

    def __init__(self):
        super().__init__()
        self.spark = self.get_spark()

    def get_raw_stock_prices_df(self, filter_condition: str = "") -> DataFrame:
        df = self.spark.read.format("iceberg").load(self.TABLE_NAME)
        if filter_condition:
            return df.filter(filter_condition)
        return df

    def write_raw_stock_prices_df(self, df: DataFrame) -> None:
        """
        Idempotent write via dynamic partition overwrite.

        Replaces MERGE INTO (which reads + joins ALL matching target partitions)
        with a much cheaper pattern:
          1. Deduplicate source rows on (ticker, trade_date)
          2. Repartition by ticker — ensures each Spark task writes 1 ticker's data.
             FanoutDataWriter then opens at most N_unique_dates compressors per task
             (e.g. 60 for 60-day lookback, 365 for a year) instead of
             N_tickers × N_dates (6300+ for 105 tickers × 60 days).
          3. Dynamic overwrite atomically replaces only the (date, ticker)
             partition files that appear in the new data — other partitions
             are untouched. Re-running is fully idempotent.
        """
        from pyspark.sql import functions as F  # noqa: PLC0415

        (
            df.dropDuplicates(["ticker", "trade_date"])
            # One Spark task per ticker: each task handles 1 ticker × N dates.
            # FanoutDataWriter opens at most N_dates compressors (not N_tickers × N_dates).
            .repartition(F.col("ticker"))
            .write.format("iceberg")
            .mode("overwrite")
            .option("overwrite-mode", "dynamic")
            .save(self.TABLE_NAME)
        )

    @classmethod
    def get_schema(cls):
        return StructType(
            [
                StructField("ticker", StringType(), False),
                StructField("exchange", StringType(), False),
                StructField("trade_date", DateType(), False),
                StructField("open", DecimalType(10, 2), False),
                StructField("high", DecimalType(10, 2), False),
                StructField("low", DecimalType(10, 2), False),
                StructField("close", DecimalType(10, 2), False),
                StructField("adj_close", DecimalType(10, 2), True),
                StructField("volume", LongType(), False),
                StructField("ingestion_ts", TimestampType(), False),
                StructField("source_batch_id", StringType(), False),
            ]
        )


class DimTickers(SparkHandler):
    TABLE_NAME = f"{app_config.catalog}.finlake_bronze.dim_tickers"
    PARTITION_BY = None
    TABLE_PROPERTIES = {
        "format-version": "2",
        "write.parquet.compression-codec": "snappy",
        "write.distribution-mode": "none",
    }

    def __init__(self):
        super().__init__()
        self.spark = self.get_spark()

    def get_dim_tickers_df(self) -> DataFrame:
        return self.spark.read.format("iceberg").load(self.TABLE_NAME)

    def write_dim_tickers_df(self, df: DataFrame) -> None:
        df.write.format("iceberg").mode("overwrite").save(self.TABLE_NAME)

    @classmethod
    def get_schema(cls):
        return StructType(
            [
                StructField("ticker", StringType(), False),
                StructField("company_name", StringType(), False),
                StructField("exchange", StringType(), False),
                StructField("yfinance_symbol", StringType(), False),
                StructField("added_date", DateType(), False),
            ]
        )
