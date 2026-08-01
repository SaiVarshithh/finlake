"""
Daily Bronze stock-price ingest.

This DAG submits the `bronze_stock_writer.py` Spark job on Kubernetes. Tickers
come from the Airflow Variable `finlake_tickers`; `lookback_days` is exposed as
a DAG parameter in the Airflow trigger UI.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import timedelta
import warnings

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from kubernetes import client, config
from kubernetes.client import ApiException


warnings.filterwarnings("ignore", category=DeprecationWarning)

NAMESPACE = os.getenv("FINLAKE_K8S_NAMESPACE", "finlake")
SPARK_IMAGE = os.getenv("FINLAKE_SPARK_IMAGE", "ghcr.io/saivarshithh/finlake-spark:latest")
MINIO_CLIENT_IMAGE = os.getenv("FINLAKE_MINIO_CLIENT_IMAGE", "quay.io/minio/mc:latest")

SPARK_EXTRA_ARGS = " ".join(
    [
        "--conf",
        "spark.executor.instances=1",
        "--conf",
        f"spark.kubernetes.container.image={SPARK_IMAGE}",
        "--conf",
        f"spark.kubernetes.namespace={NAMESPACE}",
        "--conf",
        "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
        "--conf",
        "spark.kubernetes.executor.label.app=finlake-spark",
        "--conf",
        "spark.kubernetes.executor.label.spark-pipeline=bronze-stock-prices",
        # ── Iceberg / catalog ────────────────────────────────────────────────────
        "--conf",
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf",
        "spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog",
        "--conf",
        "spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog",
        "--conf",
        "spark.sql.catalog.nessie.uri=http://finlake-nessie:19120/api/v1",
        "--conf",
        "spark.sql.catalog.nessie.ref=main",
        "--conf",
        "spark.sql.catalog.nessie.authentication.type=NONE",
        "--conf",
        "spark.sql.catalog.nessie.warehouse=s3://finlake-warehouse/warehouse",
        "--conf",
        "spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "--conf",
        "spark.sql.catalog.nessie.s3.endpoint=http://finlake-minio:9000",
        "--conf",
        "spark.sql.catalog.nessie.s3.path-style-access=true",
        "--conf",
        "spark.sql.catalog.nessie.cache-enabled=false",
        "--conf",
        "spark.sql.catalog.nessie.client.region=us-east-1",
        "--conf",
        "spark.sql.catalog.nessie.s3.access-key-id=minioadmin",
        "--conf",
        "spark.sql.catalog.nessie.s3.secret-access-key=minioadmin",
        # ── Memory / OOM fixes ───────────────────────────────────────────────────
        # Extra non-JVM memory (S3 I/O buffers, shuffle network, etc.)
        "--conf",
        "spark.executor.memoryOverhead=512m",
        # SNAPPY is heap-friendly: fixed 32KB buffer vs GZIP's multi-MB ByteArrayOutputStream
        "--conf",
        "spark.sql.parquet.compression.codec=snappy",
        # Disable FanoutWriter (keeps 1 writer/partition open simultaneously per task)
        # With 103 ticker partitions that exhausts heap. Sorted write uses 1 writer at a time.
        "--conf",
        "spark.sql.iceberg.fanout.enabled=false",
        # Low row count (~1500 rows, 103 tickers) — default 200 shuffle partitions is wasteful
        "--conf",
        "spark.sql.shuffle.partitions=4",
    ]
)


def load_kubernetes_config() -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def job_name(prefix: str, run_id: str) -> str:
    digest = hashlib.sha1(run_id.encode("utf-8")).hexdigest()[:10]
    return f"{prefix}-{digest}"[:63].rstrip("-")


def print_job_logs(core_api: client.CoreV1Api, name: str) -> None:
    pods = core_api.list_namespaced_pod(
        namespace=NAMESPACE,
        label_selector=f"job-name={name}",
    ).items
    for pod in pods:
        pod_name = pod.metadata.name
        try:
            logs = core_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=NAMESPACE,
                tail_lines=300,
            )
            print(f"----- logs from {pod_name} -----")
            print(logs)
        except ApiException as exc:
            print(f"Could not read logs for {pod_name}: {exc}")


def wait_for_job(
    batch_api: client.BatchV1Api,
    core_api: client.CoreV1Api,
    name: str,
    timeout_seconds: int,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        status = batch_api.read_namespaced_job_status(name=name, namespace=NAMESPACE).status
        if status.succeeded:
            print(f"Kubernetes Job {name} succeeded.")
            print_job_logs(core_api, name)
            return
        if status.failed and status.failed > 0:
            print_job_logs(core_api, name)
            raise AirflowException(f"Kubernetes Job {name} failed.")
        print(f"Waiting for Kubernetes Job {name}...")
        time.sleep(10)

    print_job_logs(core_api, name)
    raise AirflowException(f"Kubernetes Job {name} did not finish within {timeout_seconds} seconds.")


def create_job(batch_api: client.BatchV1Api, body: client.V1Job) -> None:
    try:
        batch_api.create_namespaced_job(namespace=NAMESPACE, body=body)
    except ApiException as exc:
        if exc.status != 409:
            raise
        batch_api.delete_namespaced_job(
            name=body.metadata.name,
            namespace=NAMESPACE,
            propagation_policy="Background",
        )
        time.sleep(5)
        batch_api.create_namespaced_job(namespace=NAMESPACE, body=body)


def resolve_tickers_csv() -> str:
    raw = Variable.get("finlake_tickers", default_var="").strip()
    if not raw:
        return ""

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = raw

    if isinstance(parsed, list):
        tickers = [str(item).strip().upper() for item in parsed if str(item).strip()]
    else:
        tickers = [item.strip().upper() for item in str(parsed).split(",") if item.strip()]

    if not tickers:
        raise AirflowException("Airflow Variable finlake_tickers is set but contains no tickers.")
    return ",".join(tickers)


def resolve_lookback_days(**context) -> int:
    value = context["params"].get("lookback_days", 20)
    try:
        lookback_days = int(value)
    except (TypeError, ValueError) as exc:
        raise AirflowException(f"lookback_days must be an integer, got {value!r}") from exc
    if lookback_days < 1 or lookback_days > 366:
        raise AirflowException("lookback_days must be between 1 and 366.")
    return lookback_days


def ensure_minio_bucket(**context) -> None:
    load_kubernetes_config()
    batch_api = client.BatchV1Api()
    core_api = client.CoreV1Api()
    name = job_name("finlake-minio-bucket", context["run_id"])

    body = client.V1Job(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=NAMESPACE,
            labels={"app": "finlake-minio-init"},
        ),
        spec=client.V1JobSpec(
            backoff_limit=2,
            ttl_seconds_after_finished=600,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": "finlake-minio-init"}),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name="mc",
                            image=MINIO_CLIENT_IMAGE,
                            command=["/bin/sh", "-ec"],
                            args=[
                                "mc alias set finlake http://finlake-minio:9000 minioadmin minioadmin && "
                                "mc mb --ignore-existing finlake/finlake-warehouse && "
                                "mc ls finlake/finlake-warehouse"
                            ],
                        )
                    ],
                ),
            ),
        ),
    )

    create_job(batch_api, body)
    wait_for_job(batch_api, core_api, name, timeout_seconds=300)


def submit_bronze_stock_job(**context) -> None:
    load_kubernetes_config()
    batch_api = client.BatchV1Api()
    core_api = client.CoreV1Api()
    name = job_name("finlake-bronze-stocks", context["run_id"])
    tickers_csv = resolve_tickers_csv()
    lookback_days = resolve_lookback_days(**context)

    env = [
        client.V1EnvVar(
            name="MY_POD_IP",
            value_from=client.V1EnvVarSource(
                field_ref=client.V1ObjectFieldSelector(field_path="status.podIP")
            ),
        ),
        client.V1EnvVar(name="SPARK_MASTER", value="k8s://https://kubernetes.default.svc:443"),
        client.V1EnvVar(name="SPARK_APP_NAME", value="finlake-bronze-stock-prices"),
        client.V1EnvVar(name="SPARK_JOB_FILE", value="/opt/spark-jobs/bronze_stock_writer.py"),
        client.V1EnvVar(name="SPARK_DRIVER_MEMORY", value="1g"),
        client.V1EnvVar(name="SPARK_EXECUTOR_MEMORY", value="2g"),
        client.V1EnvVar(name="SPARK_EXECUTOR_CORES", value="1"),
        client.V1EnvVar(name="SPARK_EXTRA_ARGS", value=SPARK_EXTRA_ARGS),
        client.V1EnvVar(name="AWS_ACCESS_KEY_ID", value="minioadmin"),
        client.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value="minioadmin"),
        client.V1EnvVar(name="AWS_REGION", value="us-east-1"),
        client.V1EnvVar(name="AWS_DEFAULT_REGION", value="us-east-1"),
        client.V1EnvVar(name="ICEBERG_CATALOG", value="nessie"),
        client.V1EnvVar(name="AIRFLOW_RUN_ID", value=context["run_id"]),
        client.V1EnvVar(name="FINLAKE_MARKET_TZ", value="Asia/Kolkata"),
        client.V1EnvVar(name="LOOKBACK_DAYS", value=str(lookback_days)),
    ]
    if tickers_csv:
        env.append(client.V1EnvVar(name="FINLAKE_TICKERS", value=tickers_csv))

    body = client.V1Job(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=NAMESPACE,
            labels={"app": "finlake-spark", "spark-pipeline": "bronze-stock-prices"},
        ),
        spec=client.V1JobSpec(
            backoff_limit=1,
            ttl_seconds_after_finished=3600,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={
                        "app": "finlake-spark",
                        "spark-role": "driver",
                        "spark-pipeline": "bronze-stock-prices",
                    }
                ),
                spec=client.V1PodSpec(
                    service_account_name="spark-sa",
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name="spark-driver",
                            image=SPARK_IMAGE,
                            image_pull_policy="Always",
                            env=env,
                            resources=client.V1ResourceRequirements(
                                requests={"cpu": "250m", "memory": "512Mi"},
                                limits={"cpu": "1", "memory": "1Gi"},
                            ),
                        )
                    ],
                ),
            ),
        ),
    )

    print(f"Submitting Bronze stock ingest for lookback_days={lookback_days}, tickers={tickers_csv or 'default'}")
    create_job(batch_api, body)
    wait_for_job(batch_api, core_api, name, timeout_seconds=1800)


with DAG(
    dag_id="finlake_bronze_stock_prices_daily",
    description="Daily yfinance NSE/BSE OHLCV ingest into Iceberg Bronze raw_stock_prices.",
    schedule="30 18 * * 1-5",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args={"owner": "finlake", "retries": 0},
    params={
        "lookback_days": Param(
            20,
            type="integer",
            minimum=1,
            maximum=366,
            description="Calendar days to pull ending at tomorrow in Asia/Kolkata. Use 183+ for a backfill.",
        )
    },
    tags=["finlake", "bronze", "stocks", "yfinance", "iceberg"],
) as dag:
    ensure_warehouse_bucket = PythonOperator(
        task_id="ensure_minio_warehouse_bucket",
        python_callable=ensure_minio_bucket,
    )

    submit_stock_ingest = PythonOperator(
        task_id="submit_bronze_stock_ingest",
        python_callable=submit_bronze_stock_job,
    )

    ensure_warehouse_bucket >> submit_stock_ingest
