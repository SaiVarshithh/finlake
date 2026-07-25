import os
from dataclasses import dataclass



@dataclass(frozen=True)
class AppConfig:
    app_name: str = "finlake"
    catalog: str = "nessie"
    nessie_uri: str = "http://finlake-nessie:19120/api/v1"
    nessie_ref: str = "main"
    nessie_auth_type: str = "NONE"
    iceberg_warehouse: str = "s3://finlake-warehouse/warehouse"
    s3_endpoint: str = "http://finlake-minio:9000"
    aws_region: str = "us-east-1"
    aws_access_key_id: str = "minioadmin"
    aws_secret_access_key: str = "minioadmin"

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            nessie_uri=os.getenv("NESSIE_URI", "http://finlake-nessie:19120/api/v1"),
            nessie_ref=os.getenv("NESSIE_REF", "main"),
            nessie_auth_type=os.getenv("NESSIE_AUTH_TYPE", "NONE"),
            iceberg_warehouse=os.getenv("ICEBERG_WAREHOUSE", "s3://finlake-warehouse/warehouse"),
            s3_endpoint=os.getenv("S3_ENDPOINT", "http://finlake-minio:9000"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )


def get_app_config() -> AppConfig:
    return AppConfig.from_env()
