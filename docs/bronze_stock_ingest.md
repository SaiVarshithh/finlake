# Bronze Stock Ingest Runbook

## What Runs

Use the Airflow DAG `finlake_bronze_stock_prices_daily`.

It runs Monday-Friday at 18:30 Asia/Kolkata and submits:

```text
/opt/spark-jobs/bronze_stock_writer.py
```

The Spark job writes to:

```text
nessie.finlake_bronze.raw_stock_prices
nessie.finlake_bronze.dim_tickers
```

## Airflow UI Configuration

Set the ticker list in Airflow Admin -> Variables:

```json
["RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK", "LT", "AXISBANK", "BAJFINANCE", "ASIANPAINT", "MARUTI", "SUNPHARMA", "TITAN", "ULTRACEMCO", "WIPRO", "NESTLEIND"]
```

Variable name:

```text
finlake_tickers
```

If `finlake_tickers` is not set, the Spark job falls back to the 20-ticker
reference list in `spark/model/constants.py`.

When manually triggering the DAG, set `lookback_days` in the Airflow trigger
form. For example:

```json
{"lookback_days": 20}
```

That pulls the rolling 20-calendar-day window ending at tomorrow in
Asia/Kolkata. Re-running the same trigger is idempotent because the write path
uses Iceberg `MERGE INTO` on `(ticker, trade_date)`.

## Backfill

For a 6-month backfill, manually trigger `finlake_bronze_stock_prices_daily`
with:

```json
{"lookback_days": 183}
```

For a 12-month backfill, use:

```json
{"lookback_days": 366}
```

The writer also supports explicit Spark-job environment variables for
one-off backfills:

```text
BACKFILL_START=2026-01-01
BACKFILL_END=2026-07-01
```

`BACKFILL_END` follows yfinance's convention and is exclusive.

## Verification

After a run, submit the verification job with the Spark image:

```powershell
kubectl run finlake-bronze-verify `
  --image=ghcr.io/saivarshithh/finlake-spark:latest `
  -n finlake `
  --restart=Never `
  --env SPARK_MASTER=local[*] `
  --env SPARK_JOB_FILE=/opt/spark-jobs/verify_bronze_stock_prices.py
```

Expected checks:

- duplicate `(ticker, trade_date)` count is zero
- each configured ticker has rows
- per-ticker first and last trade dates cover the requested lookback window
