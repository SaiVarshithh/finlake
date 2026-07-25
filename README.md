# FinLake — BFSI Data Platform

FinLake is a modern financial data platform designed for processing BFSI (Banking, Financial Services, and Insurance) transaction workloads. This repository implements an orchestrator-led lakehouse ingestion pipeline using **Apache Spark**, **Apache Iceberg**, **Project Nessie**, and **MinIO**, fully running on **Kubernetes**.

---

## Ingest Pipeline Overview

At the core of the platform is an Airflow DAG named `finlake_iceberg_spark_ingest`. When triggered, the end-to-end ingestion pipeline performs the following steps:

1. **Warehouse Initialization:** Spawns a Kubernetes pod to ensure the MinIO bucket `finlake-warehouse` exists.
2. **Spark Job Submission:** Submits a Kubernetes Job to run `spark-submit` for the ingestion script.
3. **Distributed Processing:** Launches a dedicated Spark driver pod which coordinates executor pods to distribute the workload.
4. **Lakehouse Write:** Generates and writes 100,000 deterministic test transaction records directly to the target Iceberg table: `nessie.finlake_bronze.transactions_test`.

### Data Platform Configurations

* **Iceberg Warehouse Path:** `s3://finlake-warehouse/warehouse`
* **Nessie Metadata Catalog:** Host URL `http://finlake-nessie:19120/api/v1` (tracks table history and version branches)
* **MinIO Object Storage:** API Endpoint `http://finlake-minio:9000` (stores the raw data files and metadata specifications)

---

## Deployment & Cluster Operations

This project utilizes a automated CI/CD setup. All container images (Spark, Airflow) are built and pushed to the registry automatically using **GitHub Workflows** on each commit. To deploy or update your components in the Kubernetes cluster, you only need to apply the manifests and trigger rollouts.

### 1. Apply Manifests & Restart Services

Apply the Spark RBAC permissions, refresh the Airflow deployments, and restart Airflow to pull the latest baked-in DAG image:

```powershell
# Apply Kubernetes manifests
kubectl apply -f spark/spark-rbac.yaml
kubectl apply -f airflow/airflow-k8s.yaml

# Restart Airflow to pull the latest image version from the registry
kubectl rollout restart deploy/finlake-airflow -n finlake
```

### 2. Monitor Spark Job & Pod Logs

Once you trigger the `finlake_iceberg_spark_ingest` DAG in the Airflow UI, you can inspect the running Kubernetes pods and fetch executor logs:

```powershell
# Track active Spark job execution and driver pod state
kubectl get jobs,pods -n finlake -l spark-pipeline=iceberg-transactions

# View logs for the Spark driver pod
kubectl logs -n finlake -l app=finlake-spark,spark-role=driver --tail=200

# Monitor dynamically scaled executor pods
kubectl get pods -n finlake -l spark-role=executor
```

---

## Architectural Breakdown

The system is split into four decoupled components designed to scale independently:

```mermaid
graph TD
    A[Airflow DAG] -->|Task 1: Create Bucket| B(MinIO API)
    A -->|Task 2: Submit K8s Job| C[Spark Driver Pod]
    C -->|Creates| D[Spark Executor Pods]
    C & D -->|Registers metadata| E[Nessie Catalog]
    C & D -->|Writes Parquet Files| F[MinIO S3 Bucket]
```

### A. Orchestration (Airflow)
* The pipeline starts inside an Airflow environment running in **Standalone Mode** (Scheduler, Executor, and Webserver run in a single process inside a single pod for simplified resource footprint).
* The custom [Dockerfile](./airflow/Dockerfile) builds upon Airflow `2.9.3`, pre-installing the required dependencies.
* The [Airflow Deployment](./airflow/airflow-k8s.yaml) sets up the pod running `airflow standalone`.
* The [Spark Job DAG](./airflow/dags/finlake_iceberg_spark_dag.py) coordinates two sequential tasks:
  1. **MinIO Bucket Creation (`mc`):** Executes a startup job using the MinIO client image to run the bucket check command:
     ```bash
     mc alias set finlake http://finlake-minio:9000 minioadmin minioadmin && \
     mc mb --ignore-existing finlake/finlake-warehouse && \
     mc ls finlake/finlake-warehouse
     ```
  2. **Job Submission:** Submits a Kubernetes job pointing to the PySpark entrypoint. This task only launches once the bucket check returns successfully.

### B. Distributed Processing (Spark)
* When the DAG triggers `spark-submit`, a Spark Driver pod starts up. The driver reads the custom [entrypoint.sh](./spark/entrypoint.sh) to handle the startup sequence.
* Spark automatically starts worker pods using the same image. Because of this, the entrypoint script performs an argument check:
  ```mermaid
  graph TD
      Entry["entrypoint.sh"] --> IsExecutor{Is $1 == 'executor'?}
      IsExecutor -->|Yes| Exec[Start CoarseGrainedExecutorBackend]
      IsExecutor -->|No| Driver[Build spark-submit Command & Run]
  ```
* **Executor Branch:** Starts the internal Java executor process (`CoarseGrainedExecutorBackend`) to start processing tasks.
* **Driver Branch:** Assembles the full command line arguments and runs `spark-submit` to start the [PySpark Script](./spark/jobs/iceberg_transactions_job.py). The driver coordinates partitions, assigns tasks to executors, and monitors progress.

### C. Data Ingestion & Storage Layout
* When writing data, Spark first ensures the namespace (`finlake_bronze`) exists, creates the table target, and writes the contents.
* **Physical Layer:** Spark partitions data by `processing_date` and uploads the columnar Parquet files directly to S3 storage at:
  `s3://finlake-warehouse/warehouse/finlake_bronze/transactions_test/data/`
* **Metadata Layer:** Alongside data, Spark writes Iceberg manifest and structural files detailing schemas and partition keys under:
  `s3://finlake-warehouse/warehouse/finlake_bronze/transactions_test/metadata/`
* **Catalog Registration:** Once the physical files are written, the Spark driver submits a transaction commit to **Nessie**, updating the metadata pointer.

```
       [ Nessie Catalog Server ]
             |          |
      Commit |          | Pointer
             v          v
   [ Metadata JSON ]  [ Metadata JSON ]
   (Snapshot 1)       (Snapshot 2)
```

---

### 3. Verification

For verification of the deployed architecture, you can use the following commands:

```bash
kubectl run finlake-verify-debug `
  --image=ghcr.io/saivarshithh/finlake-spark:latest `
  --namespace=finlake `
  --restart=Never `
  --overrides='{"spec": {"containers": [{"name": "spark-verify", "image": "ghcr.io/saivarshithh/finlake-spark:latest", "command": ["sleep", "3600"]}]}}'
```

This will deploy a pod with the image `ghcr.io/saivarshithh/finlake-spark:latest` in the `finlake` namespace. Once the pod is running, we can copy the verification script inside the pod.

```bash
kubectl cp ./spark/jobs/verify_iceberg_job.py finlake-verify-debug:/tmp/verify_iceberg_job.py -n finlake
```
Then, we can execute the verification script inside the pod.

```bash
kubectl exec -it -n finlake finlake-verify-debug -- /bin/bash
spark-submit --master "local[*]" /tmp/verify_iceberg_job.py
```

## Architectural Rationale: Why Nessie & MinIO?

### 1. Why Project Nessie over Hive Metastore?
For traditional data lakes, **Hive Metastore (HMS)** has long been the default choice. However, modern transactional lakehouses have requirements that HMS was never designed to solve. We selected Nessie for three key advantages:
* **Git-like Version Control for Data:** Nessie introduces branches (`main`, `dev`), tags, and merges to your tables. You can run ETL changes on a separate branch, test them, and merge them into production instantly and atomically—without copying or moving any physical data files.
* **Lightweight and Database-Free:** Hive Metastore requires running a heavy relational database (like PostgreSQL or MySQL) to maintain schemas and partitions. Nessie runs as a lightweight, API-first service with transactional log storage.
* **Atomic Multi-Table Transactions:** Nessie can commit changes across multiple tables simultaneously. If you write data to both a facts and dimensions table, Nessie guarantees downstream users will see either both updates or neither, preventing inconsistent partial reads.

### 2. Why MinIO?
* **Local S3 Compatibility:** MinIO runs as a lightweight service in our local Kubernetes cluster while exposing the exact same S3 API endpoints as AWS.
* **Seamless Cloud Portability:** Because Spark and Iceberg talk to MinIO using standard AWS SDK parameters, you can move this entire pipeline to a production cloud environment (AWS S3) without changing a single line of PySpark code.