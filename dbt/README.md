# FinLake dbt project

This project owns SQL transformations after Spark has landed raw data in the
Bronze Iceberg tables. dbt compiles the models and submits SQL to Trino; Trino
reads and writes Iceberg data through the Nessie catalog.

## Model flow

One `dbt build` creates and tests this lineage:

```text
iceberg.finlake_bronze.raw_stock_prices
  -> stg_stock_prices (ephemeral SQL, no table)
  -> iceberg.finlake_silver.clean_prices (table)
     -> iceberg.finlake_gold.daily_returns (table)
        -> iceberg.finlake_gold.volatility_7d_30d (table)
        -> iceberg.finlake_gold.sector_performance (table)
     -> iceberg.finlake_gold.moving_avg_20_50 (table)

sector_mapping.csv
  -> iceberg.finlake_silver.sector_mapping (seed table)
     -> iceberg.finlake_gold.sector_performance
```

`clean_prices` keeps the newest ingestion for each `(ticker, trade_date)` and
the tests enforce required fields, uniqueness, positive prices, valid daily
high/low bounds, non-negative volume, and no future trade dates. Gold publishes
close-to-close returns, full-window 7/30-observation volatility, full-window
20/50-observation moving averages, and equal-weighted sector performance.

The sector seed covers the 20 tickers currently enabled in
`spark/model/constants.py`. Unmapped configured tickers produce a dbt warning
and are excluded from sector aggregation until the seed is extended; the other
Gold models still include them.

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
| `DBT_TRINO_SCHEMA`      | `finlake`                                       | Base schema; dbt appends layer name |
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
connection, while `dbt build` loads seeds, creates models in dependency order,
and then runs their tests.

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
  -> run_dbt_analytics_build
```

The final task creates a `finlake-dbt-build-*` Kubernetes Job in the `finlake`
namespace and waits for it at 10-second intervals. The Job runs
`dbt build --fail-fast`, requests 128Mi memory, is limited to 512Mi, and is
cleaned up one hour after completion. A failed seed, model, or error-level test
makes both the Job and Airflow task fail.

By default, the DAG derives the dbt image from `FINLAKE_SPARK_IMAGE`. For
example, a Spark image ending in `:abc123` selects the dbt image ending in the
same `:abc123`, matching the images produced together by GitHub Actions.
`FINLAKE_DBT_IMAGE` can override this when required.
