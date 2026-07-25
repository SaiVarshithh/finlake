from pyspark.sql import SparkSession
from config.app_config import get_app_config

app_config = get_app_config()


def build_spark() -> SparkSession:
    catalog = app_config.catalog
    builder = (
        SparkSession.builder.appName("finlake-bronze-stock-writer")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config(f"spark.sql.catalog.{catalog}.uri", app_config.nessie_uri)
        .config(f"spark.sql.catalog.{catalog}.ref", app_config.nessie_ref)
        .config(f"spark.sql.catalog.{catalog}.authentication.type", app_config.nessie_auth_type)
        .config(f"spark.sql.catalog.{catalog}.warehouse", app_config.iceberg_warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", app_config.s3_endpoint)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.cache-enabled", "false")
        .config(f"spark.sql.catalog.{catalog}.client.region", app_config.aws_region)
        .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", app_config.aws_access_key_id)
        .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", app_config.aws_secret_access_key)
    )

    return builder.getOrCreate()
