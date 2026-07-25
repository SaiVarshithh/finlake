from pyspark.sql.types import (
    StructType, StructField, StringType,
    DateType, LongType, TimestampType, DecimalType
)
from pyspark.sql import DataFrame
from config.app_config import get_app_config

app_config = get_app_config()

class RawStockPrices:
    TABLE_NAME = f"{app_config.catalog}.finlake_bronze.raw_stock_prices"
    PARTITION_BY = "months(trade_date)"
    TABLE_PROPERTIES = {
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
        "write.distribution-mode": "hash"
    }

    def __init__(self):
        super().__init__()
        self.spark = self.get_spark()

    def get_raw_stock_prices_df(self, filter_condition:str = "") -> DataFrame:
        if filter_condition != "":
            return self.spark.read.format("iceberg").load(self.TABLE_NAME).filter(filter_condition)
        else:
            return self.spark.read.format("iceberg").load(self.TABLE_NAME)

    def write_raw_stock_prices_df(self, df: DataFrame) -> None:
        df.write.format("iceberg").mode("append").partitionBy("trade_date").save(self.TABLE_NAME)

    @classmethod
    def get_schema(cls):
        schema = StructType([
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
            StructField("source_batch_id", StringType(), False)
        ])
        return schema


class DimTickers:
    TABLE_NAME = f"{app_config.catalog}.finlake_bronze.dim_tickers"
    PARTITION_BY = None
    TABLE_PROPERTIES = {
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
        "write.distribution-mode": "hash"
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
        schema = StructType([
            StructField("ticker", StringType(), False),
            StructField("company_name", StringType(), False),
            StructField("exchange", StringType(), False),
            StructField("yfinance_symbol", StringType(), False),
            StructField("added_date", DateType(), False)
        ])
        return schema
