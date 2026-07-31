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
        "write.parquet.compression-codec": "zstd",
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
        Idempotent write via MERGE INTO on (ticker, trade_date). Re-running a
        date/ticker combination updates that row instead of inserting a
        duplicate; other partitions are untouched.
        """
        source_view = "raw_stock_prices_source"
        df.dropDuplicates(["ticker", "trade_date"]).createOrReplaceTempView(source_view)
        self.spark.sql(
            f"""
            MERGE INTO {self.TABLE_NAME} t
            USING {source_view} s
            ON t.ticker = s.ticker AND t.trade_date = s.trade_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
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
        "write.parquet.compression-codec": "zstd",
        "write.distribution-mode": "hash",
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
