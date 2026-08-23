# FinLake dbt project

This project owns SQL transformations after Spark has landed raw data in the
Bronze Iceberg tables. dbt compiles the models and submits SQL to Trino; Trino
reads and writes Iceberg data through the Nessie catalog.

## Current scope

The first slice creates this lineage:

```text
iceberg.finlake_bronze.raw_stock_prices
  -> stg_stock_prices (ephemeral SQL, no table)
  -> iceberg.finlake_silver.clean_prices (table)
```

`clean_prices` keeps the newest ingestion for each `(ticker, trade_date)` and
the tests enforce required fields, uniqueness, positive prices, valid daily
high/low bounds, non-negative volume, and no future trade dates.

The Airflow Bronze DAG launches this project as a short-lived Kubernetes Job
after the Spark ingestion succeeds. Seeds, Gold models, and incremental
materializations remain deferred until the Silver contract is established.

## Configuration

The committed `profiles.yml` contains no password and reads connection values
from environment variables. Its defaults work from a pod in the `finlake`
namespace:

| Variable                  | Default                                           | Purpose                             |
| ------------------------- | ------------------------------------------------- | ----------------------------------- |
| `DBT_TRINO_HOST`        | `finlake-trino-trino.finlake.svc.cluster.local` | Trino service                       |
| `DBT_TRINO_PORT`        | `8080`                                          | Trino HTTP port                     |
| `DBT_TRINO_USER`        | `finlake_dbt`                                   | Trino query identity                |
| `DBT_TRINO_CATALOG`     | `iceberg`                                       | Trino catalog                       |
| `DBT_TRINO_SCHEMA`      | `finlake`                                       | Base schema; dbt appends`_silver` |
| `DBT_TRINO_HTTP_SCHEME` | `http`                                          | Trino connection scheme             |
| `DBT_BRONZE_SCHEMA`     | `finlake_bronze`                                | Existing Spark-owned Bronze schema  |

For local development, forward Trino and override only host and port:

```powershell
kubectl -n finlake port-forward svc/finlake-trino-trino 8081:8080
$env:DBT_TRINO_HOST = "localhost"
$env:DBT_TRINO_PORT = "8081"
```

## Commands

Run commands from this directory. `dbt debug` checks the profile and Trino
connection, while `dbt build` creates models in dependency order and then runs
their tests.

```powershell
python -m pip install -r requirements.txt
dbt debug
dbt build
dbt docs generate
dbt docs serve
```

The container uses the same project and profile:

```powershell
docker build -t finlake-dbt -f dbt/Dockerfile dbt
docker run --rm finlake-dbt parse
```

## Scheduled execution

`finlake_bronze_stock_prices_daily` runs the following dependency chain:

```text
ensure_minio_warehouse_bucket
  -> submit_bronze_stock_ingest
  -> run_dbt_silver_build
```

The final task creates a `finlake-dbt-silver-*` Kubernetes Job in the `finlake`
namespace and waits for it at 10-second intervals. The Job runs `dbt build --fail-fast`, requests 128Mi memory, is limited to 512Mi, and is cleaned up one
hour after completion. A failed model or test makes both the Job and Airflow
task fail.

By default, the DAG derives the dbt image from `FINLAKE_SPARK_IMAGE`. For
example, a Spark image ending in `:abc123` selects the dbt image ending in the
same `:abc123`, matching the images produced together by GitHub Actions.
`FINLAKE_DBT_IMAGE` can override this when required.
