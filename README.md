# FinLake - BFSI Data Platform

## Airflow-triggered Spark Iceberg ingest

This repo includes an Airflow DAG named `finlake_iceberg_spark_ingest`.
When triggered, it:

1. Creates the MinIO bucket `finlake-warehouse` if it does not exist.
2. Starts a Kubernetes Job running `spark-submit`.
3. Uses the Kubernetes Spark driver pod plus 2 executor pods.
4. Writes 100,000 deterministic test transaction records to:
   `nessie.finlake_bronze.transactions_test`

The Iceberg warehouse is `s3://finlake-warehouse/warehouse`, catalog metadata is
tracked in Nessie at `http://finlake-nessie:19120/api/v1`, and data files are
stored in MinIO at `http://finlake-minio:9000`.

### Deploy updated images and manifests

```powershell
docker build -f spark/Dockerfile -t ghcr.io/saivarshithh/finlake-spark:latest ./spark
docker build -f airflow/Dockerfile -t ghcr.io/saivarshithh/finlake-airflow:latest ./airflow

docker push ghcr.io/saivarshithh/finlake-spark:latest
docker push ghcr.io/saivarshithh/finlake-airflow:latest

kubectl apply -f spark/spark-rbac.yaml
kubectl apply -f airflow/airflow-k8s.yaml
kubectl rollout restart deploy/finlake-airflow -n finlake
```

Open Airflow and trigger `finlake_iceberg_spark_ingest`.

Useful checks:

```powershell
kubectl get jobs,pods -n finlake -l spark-pipeline=iceberg-transactions
kubectl logs -n finlake -l app=finlake-spark,spark-role=driver --tail=200
kubectl get pods -n finlake -l spark-role=executor
```
