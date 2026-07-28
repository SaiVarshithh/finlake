"""
FinLake — Base class for Iceberg-backed Spark table models.

Subclasses (RawStockPrices, DimTickers, etc. in spark/model/models.py) inherit
from this to get a shared, correctly-configured SparkSession accessor. The
iceberg_initialisation decorator discovers subclasses of SparkHandler via
issubclass() checks — a class must inherit from this to be auto-discovered
and have its Iceberg table created.
"""

from pyspark.sql import SparkSession


class SparkHandler:
    """Marker/base class for FinLake Iceberg table models.

    Provides a shared way to obtain the active SparkSession. Table metadata
    (TABLE_NAME, PARTITION_BY, TABLE_PROPERTIES, get_schema()) is defined
    per-subclass and consumed by iceberg_initialisation for auto-discovery
    and DDL generation.
    """

    @staticmethod
    def get_spark() -> SparkSession:
        # Returns the existing session (already configured by build_spark()
        # earlier in the call chain) rather than building a new, unconfigured
        # one — SparkSession.builder.getOrCreate() is a singleton accessor,
        # not a constructor, once a session already exists in this JVM.
        return SparkSession.builder.getOrCreate()
