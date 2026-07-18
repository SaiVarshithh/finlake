"""
Airflow DAG that submits the FinLake Iceberg PySpark job on Kubernetes.

The DAG creates two short-lived Kubernetes Jobs:
  1. Ensure the MinIO warehouse bucket exists.
  2. Run spark-submit from the FinLake Spark image. The Spark driver runs in
     that Kubernetes Job pod and Spark creates executor pods through spark-sa.
"""

from __future__ import annotations

import hashlib
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from kubernetes import client, config
from kubernetes.client import ApiException


NAMESPACE = os.getenv("FINLAKE_K8S_NAMESPACE", "finlake")
SPARK_IMAGE = os.getenv("FINLAKE_SPARK_IMAGE", "ghcr.io/saivarshithh/finlake-spark:latest")
MINIO_CLIENT_IMAGE = os.getenv("FINLAKE_MINIO_CLIENT_IMAGE", "quay.io/minio/mc:latest")

SPARK_EXTRA_ARGS = " ".join(
    [
        "--conf",
        "spark.executor.instances=2",
        "--conf",
        f"spark.kubernetes.container.image={SPARK_IMAGE}",
        "--conf",
        f"spark.kubernetes.namespace={NAMESPACE}",
        "--conf",
        "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
        "--conf",
        "spark.kubernetes.executor.label.app=finlake-spark",
        "--conf",
        "spark.kubernetes.executor.label.spark-pipeline=iceberg-transactions",
        "--conf",
        "spark.kubernetes.executorEnv.AWS_ACCESS_KEY_ID=minioadmin",
        "--conf",
        "spark.kubernetes.executorEnv.AWS_SECRET_ACCESS_KEY=minioadmin",
        "--conf",
        "spark.kubernetes.executorEnv.AWS_REGION=us-east-1",
        "--conf",
        "spark.kubernetes.executorEnv.AWS_DEFAULT_REGION=us-east-1",
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


def submit_spark_job(**context) -> None:
    load_kubernetes_config()
    batch_api = client.BatchV1Api()
    core_api = client.CoreV1Api()
    name = job_name("finlake-iceberg-spark", context["run_id"])

    body = client.V1Job(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=NAMESPACE,
            labels={"app": "finlake-spark", "spark-pipeline": "iceberg-transactions"},
        ),
        spec=client.V1JobSpec(
            backoff_limit=1,
            ttl_seconds_after_finished=3600,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={
                        "app": "finlake-spark",
                        "spark-role": "driver",
                        "spark-pipeline": "iceberg-transactions",
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
                            env=[
                                client.V1EnvVar(
                                    name="MY_POD_IP",
                                    value_from=client.V1EnvVarSource(
                                        field_ref=client.V1ObjectFieldSelector(
                                            field_path="status.podIP"
                                        )
                                    ),
                                ),
                                client.V1EnvVar(
                                    name="SPARK_MASTER",
                                    value="k8s://https://kubernetes.default.svc:443",
                                ),
                                client.V1EnvVar(
                                    name="SPARK_APP_NAME",
                                    value="finlake-iceberg-transactions",
                                ),
                                client.V1EnvVar(
                                    name="SPARK_JOB_FILE",
                                    value="/opt/spark-jobs/iceberg_transactions_job.py",
                                ),
                                client.V1EnvVar(name="SPARK_DRIVER_MEMORY", value="1g"),
                                client.V1EnvVar(name="SPARK_EXECUTOR_MEMORY", value="1g"),
                                client.V1EnvVar(name="SPARK_EXECUTOR_CORES", value="1"),
                                client.V1EnvVar(name="SPARK_EXTRA_ARGS", value=SPARK_EXTRA_ARGS),
                                client.V1EnvVar(name="AWS_ACCESS_KEY_ID", value="minioadmin"),
                                client.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value="minioadmin"),
                                client.V1EnvVar(name="AWS_REGION", value="us-east-1"),
                                client.V1EnvVar(name="AWS_DEFAULT_REGION", value="us-east-1"),
                                client.V1EnvVar(name="ICEBERG_CATALOG", value="nessie"),
                                client.V1EnvVar(name="ICEBERG_NAMESPACE", value="finlake_bronze"),
                                client.V1EnvVar(name="ICEBERG_TABLE", value="transactions_test"),
                                client.V1EnvVar(name="TEST_RECORD_COUNT", value="100000"),
                                client.V1EnvVar(name="AIRFLOW_RUN_ID", value=context["run_id"]),
                            ],
                            resources=client.V1ResourceRequirements(
                                requests={"cpu": "500m", "memory": "1.5Gi"},
                                limits={"cpu": "2", "memory": "3Gi"},
                            ),
                        )
                    ],
                ),
            ),
        ),
    )

    create_job(batch_api, body)
    wait_for_job(batch_api, core_api, name, timeout_seconds=1800)


with DAG(
    dag_id="finlake_iceberg_spark_ingest",
    description="Create MinIO warehouse bucket and submit PySpark Iceberg ingest through Kubernetes.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args={"owner": "finlake", "retries": 0},
    tags=["finlake", "spark", "iceberg", "nessie", "minio"],
) as dag:
    ensure_warehouse_bucket = PythonOperator(
        task_id="ensure_minio_warehouse_bucket",
        python_callable=ensure_minio_bucket,
    )

    submit_iceberg_ingest = PythonOperator(
        task_id="submit_spark_iceberg_ingest",
        python_callable=submit_spark_job,
    )

    ensure_warehouse_bucket >> submit_iceberg_ingest
