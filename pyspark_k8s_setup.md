# FinLake — PySpark on Kubernetes Setup

## Files Created

| File | Purpose |
|---|---|
| `Dockerfile.pyspark` | Builds the PySpark image (bitnami/spark base) |
| `spark/entrypoint.sh` | `spark-submit` wrapper — all config via env vars |
| `spark/requirements.txt` | Python deps for your Spark jobs |
| `spark/jobs/sample_job.py` | Sample job to validate the container |
| `spark/spark-k8s.yaml` | Full k8s manifest (RBAC + ConfigMap + Job + Service) |

---

## Why `bitnami/spark` as the base?

- Ships with **Java 17 + Spark 3.5.x + Python 3.11** pre-installed — no manual JDK wrangling.
- Kubernetes-ready: Bitnami images run as non-root by default.
- Actively maintained with security patches.
- Alternative: `apache/spark:3.5.6-scala2.12-java17-python3-ubuntu` (official ASF image, slightly leaner).

---

## Step 1 — Build the Image

```bash
# From the project root (finlake/)
docker build \
  -f Dockerfile.pyspark \
  -t ghcr.io/saivarshithh/finlake-spark:latest \
  .
```

> [!NOTE]
> The COPY instructions in `Dockerfile.pyspark` reference `spark/requirements.txt` and `spark/jobs/` relative to the build context (the project root). Always run `docker build` from `finlake/`.

---

## Step 2 — Smoke Test Locally

```bash
docker run --rm \
  -e SPARK_MASTER="local[*]" \
  -e SPARK_JOB_FILE="/opt/spark/jobs/sample_job.py" \
  ghcr.io/saivarshithh/finlake-spark:latest
```

You should see the sample trades DataFrame printed in the output.

---

## Step 3 — Push to Registry

```bash
docker push ghcr.io/saivarshithh/finlake-spark:latest
```

---

## Step 4 — Deploy to Kubernetes

```bash
# Apply RBAC + ConfigMap + Job + Service in one shot
kubectl apply -f spark/spark-k8s.yaml

# Watch the Job progress
kubectl get jobs -w

# Tail driver logs
kubectl logs -l app=finlake-spark,spark-role=driver -f

# Access the Spark UI (get the NodePort assigned)
kubectl get svc finlake-spark-ui
# Open: http://<node-ip>:<NodePort>
```

---

## Step 5 — Run Your Own Job

1. Drop your `.py` file into `spark/jobs/`.
2. Rebuild and push the image.
3. Edit `spark/spark-k8s.yaml` → `ConfigMap` → `SPARK_JOB_FILE` to point to your new file.
4. Re-apply: `kubectl apply -f spark/spark-k8s.yaml`

**Or** override without rebuilding (useful for dev iterations):

```bash
kubectl run finlake-spark-dev \
  --image=ghcr.io/saivarshithh/finlake-spark:latest \
  --restart=Never \
  --env="SPARK_JOB_FILE=/opt/spark/jobs/my_job.py" \
  --env="SPARK_MASTER=local[*]"
```

---

## Running Against a Spark Standalone / k8s Native Cluster

Change `SPARK_MASTER` in the ConfigMap:

| Mode | Value |
|---|---|
| Local (testing) | `local[*]` |
| Spark Standalone | `spark://<master-host>:7077` |
| Kubernetes native | `k8s://https://<api-server>:6443` |

> [!IMPORTANT]
> For **k8s-native mode** (`k8s://...`) the driver pod creates executor pods dynamically. The `spark-sa` ServiceAccount with the ClusterRoleBinding in `spark-k8s.yaml` grants the exact permissions Spark needs for this. You'll also need to add these `--conf` flags to `SPARK_EXTRA_ARGS`:
> ```
> --conf spark.kubernetes.container.image=ghcr.io/saivarshithh/finlake-spark:latest
> --conf spark.kubernetes.namespace=default
> ```

---

## Adding Extra Packages (Delta, Kafka…)

Set `SPARK_EXTRA_ARGS` in the ConfigMap:

```yaml
SPARK_EXTRA_ARGS: >-
  --packages
  io.delta:delta-spark_2.12:3.3.1,
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6
```

> [!TIP]
> Pre-download JARs into the image (`ADD <url> /opt/spark/jars/`) to avoid network downloads at runtime on every job run — important in air-gapped or slow-network environments.

---

## Upgrade PySpark Version

Change the `ARG SPARK_VERSION` at the top of `Dockerfile.pyspark` and update `pyspark==<version>` in `spark/requirements.txt` to match.
