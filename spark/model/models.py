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
    # Daily OHLCV has one row per ticker/day. Month partitions avoid creating
    # nearly one Parquet partition and file per input row.
    PARTITION_BY = ["months(trade_date)"]
    PARTITION_EVOLUTION = [
        ("add", "months(trade_date)"),
        ("drop", "days(trade_date)"),
        ("drop", "ticker"),
    ]
    TABLE_PROPERTIES = {
        "format-version": "2",
        "write.parquet.compression-codec": "snappy",
        # hash: Spark pre-shuffles by partition key before writing.
        # Each task gets data for fewer unique partitions → fewer simultaneous compressors.
        # 'none' was WRONG — it forces FanoutDataWriter with ALL partitions open at once.
        "write.distribution-mode": "hash",
        "write.spark.fanout.enabled": "false",
        "write.target-file-size-bytes": str(64 * 1024 * 1024),
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
        """Idempotently upsert a download on (ticker, trade_date).

        Dynamic partition overwrite is unsafe with monthly partitions because
        a daily run could replace the whole month. MERGE changes only matching
        business keys. Iceberg hash-clusters the output by month, allowing the
        non-fanout writer to keep one Parquet compressor open at a time.
        """
        source_view = "_finlake_raw_stock_prices_source"
        df.dropDuplicates(["ticker", "trade_date"]).createOrReplaceTempView(source_view)
        try:
            self.spark.sql(
                f"""
                MERGE INTO {self.TABLE_NAME} AS target
                USING {source_view} AS source
                ON target.ticker = source.ticker
                   AND target.trade_date = source.trade_date
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        finally:
            self.spark.catalog.dropTempView(source_view)

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
        df.write.format("iceberg").option("check-nullability", "false").mode("overwrite").save(self.TABLE_NAME)

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
