# FinLake — Sprint 1 Detailed Requirements
## Ingestion, Storage Schema & Query Access Layer

---

## Story 1: Land NSE/BSE OHLCV data into Iceberg (append + partitioned)

### Business context
An analyst or downstream model needs a trustworthy, growing historical record of daily stock prices. The record must never lose history on re-run — that was the exact bug in the Week 1 foundation test table, and this story exists so it doesn't repeat in real data.

### Iceberg tables to create

#### Table 1: `finlake_bronze.raw_stock_prices` (primary fact table)

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `ticker` | `string` | No | e.g. `RELIANCE`, `TCS` — base symbol, no exchange suffix |
| `exchange` | `string` | No | `NSE` or `BSE` |
| `trade_date` | `date` | No | The trading day this row represents |
| `open` | `decimal(10,2)` | No | |
| `high` | `decimal(10,2)` | No | |
| `low` | `decimal(10,2)` | No | |
| `close` | `decimal(10,2)` | No | |
| `adj_close` | `decimal(10,2)` | Yes | yfinance-provided, accounts for splits/dividends |
| `volume` | `bigint` | No | |
| `ingestion_ts` | `timestamp` | No | When this row was written — your audit trail |
| `source_batch_id` | `string` | No | Airflow DAG run ID that wrote this row — lets you trace any row back to the exact run that produced it |

**DDL (Spark SQL, run through your existing Spark job or a one-time setup script):**
```sql
CREATE TABLE IF NOT EXISTS nessie.finlake_bronze.raw_stock_prices (
    ticker         STRING,
    exchange       STRING,
    trade_date     DATE,
    open           DECIMAL(10,2),
    high           DECIMAL(10,2),
    low            DECIMAL(10,2),
    close          DECIMAL(10,2),
    adj_close      DECIMAL(10,2),
    volume         BIGINT,
    ingestion_ts   TIMESTAMP,
    source_batch_id STRING
)
USING iceberg
PARTITIONED BY (months(trade_date));
```

**Why `months(trade_date)` and not `days(trade_date)`:**
At 20 tickers × 1 row/ticker/day = **20 rows per day**. Daily partitioning would create a new Parquet file (or several, on retries) for a 20-row partition — hundreds of tiny files within a year, which slows down every query that scans across dates. Monthly partitioning keeps ~420 rows (20 tickers × ~21 trading days) per partition — still small, but a sane file count. Revisit only if the ticker universe grows past a few hundred names.

**Logical relationships (not enforced — see note below):**
- Join key: `(ticker, trade_date)` — this pair should be unique per row. Iceberg does not support primary key or foreign key constraints; uniqueness must be enforced by your write logic, not the table definition.
- Logical link to `dim_tickers` (below) on `ticker` — a standard star-schema style join, done at query time in Trino/dbt, not a DB-enforced relationship.

#### Table 2: `finlake_bronze.dim_tickers` (reference/dimension table)

You need this now, not in Week 2, because your ingest job needs to know the NSE/BSE ticker list, and Trino/DBeaver users will want company names, not just symbols.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `ticker` | `string` | No | Matches `raw_stock_prices.ticker` |
| `company_name` | `string` | No | |
| `exchange` | `string` | No | `NSE` or `BSE` |
| `yfinance_symbol` | `string` | No | The actual string passed to yfinance, e.g. `RELIANCE.NS` — yfinance requires exchange suffixes, your internal `ticker` column should not carry that suffix |
| `added_date` | `date` | No | When this ticker was added to the pipeline |

**DDL:**
```sql
CREATE TABLE IF NOT EXISTS nessie.finlake_bronze.dim_tickers (
    ticker           STRING,
    company_name     STRING,
    exchange         STRING,
    yfinance_symbol  STRING,
    added_date       DATE
)
USING iceberg;
```
No partitioning needed — this table will have ~20-50 rows total, ever.

### Data volume estimate
- 20 tickers × ~21 trading days/month = **~420 rows/month** into `raw_stock_prices`
- 6-month backfill = **~2,500 rows**, 12-month = **~5,000 rows**
- Row size ≈ 80-100 bytes → total Parquet footprint for a year of data is **well under 1 MB**, even accounting for Parquet overhead. This is a trivially small dataset at this ticker count — do not over-engineer storage tiering or compaction schedules yet. Revisit compaction only once you're ingesting hundreds of tickers or intraday (Week 5 streaming) volume.

### Write strategy (idempotency — this is the part that prevents repeat bugs)
Do **not** use `INSERT INTO` blindly on every run (duplicates on retry) or blanket `OVERWRITE` (destroys other partitions' history — the Week 1 bug). Use one of:
- **`MERGE INTO`** on `(ticker, trade_date)` — update if exists, insert if not. Preferred; true idempotency.
- **Partition-scoped overwrite**: `INSERT OVERWRITE ... PARTITION (trade_date range for this run only)` — acceptable, simpler, but only safe if your job always knows exactly which date range it's writing.

### Acceptance criteria
- [ ] Both tables exist and are queryable via Spark SQL
- [ ] Running the ingest job twice for the same date does not create duplicate `(ticker, trade_date)` rows
- [ ] `dim_tickers` has all 20 tickers with correct `yfinance_symbol` mapping
- [ ] A join query `SELECT p.*, d.company_name FROM raw_stock_prices p JOIN dim_tickers d ON p.ticker = d.ticker` returns correct results

### Impacted components
New Spark job (`bronze_stock_writer.py`), two new Iceberg tables, no changes to existing `transactions_test` table (left untouched, separate concern).

### Estimated time: 3 hr (as scoped previously) — schema design above adds ~30 min to account for the `dim_tickers` table, which wasn't in the original story scope

---

## Story 2: Scheduled Airflow DAG for daily ingest

### No new tables — this is orchestration, not storage

### Requirements
- New DAG file `bronze_ingest_daily.py`, modeled on the existing `finlake_iceberg_spark_dag.py` pattern (same K8s Job submission approach — don't introduce a second orchestration pattern)
- Schedule: daily, timed after Indian market close (NSE/BSE close ~3:30 PM IST) — e.g. `schedule_interval="30 18 * * 1-5"` (6:30 PM IST, weekdays only — no point running on weekends when markets are closed)
- Ticker list read from an Airflow Variable (`finlake_tickers`, JSON array) — **not** hardcoded, so adding a ticker doesn't require a code change/redeploy
- Job passes the run's `trade_date` (or date range for catch-up runs) as a parameter to the Spark job, so the job knows its own idempotency scope

### Acceptance criteria
- [ ] DAG appears in Airflow UI with correct schedule
- [ ] Manually triggering the DAG for a specific date populates only that date's partition
- [ ] Airflow Variable `finlake_tickers` can be edited without a code deploy, and the next run picks up the change

### Impacted components
New DAG file, Airflow Variable, existing K8s Job submission helper functions (reused, not rewritten)

### Estimated time: 2 hr (unchanged from prior scope)

---

## Story 3: Historical backfill

### No new tables — same `raw_stock_prices` table, populated with a date range instead of a single day

### Requirements
- A backfill script/DAG run that loops over a historical date range (recommend 6 months to start — see volume estimate above; 12 months is the stretch goal if time allows) and writes each month's data using the same `MERGE INTO` (or partition-overwrite) logic as the daily job — **must reuse Story 1's write path, not a separate one-off script**, or you'll have two different write behaviors to debug later
- Respect the `months(trade_date)` partitioning — a full 6-month backfill will touch exactly 6 partitions

### Data volume for backfill
6 months × 20 tickers × ~21 trading days = **~2,520 rows**, landing across 6 monthly partitions (~420 rows each). Trivial volume — the backfill should complete in minutes of actual Spark runtime; your time budget here is mostly about handling yfinance rate limits/retries gracefully, not data size.

### Acceptance criteria
- [ ] `raw_stock_prices` contains 6 continuous months of data per ticker, no missing trading days
- [ ] Running the backfill twice does not duplicate rows (proves Story 1's idempotency actually works under a real stress case)
- [ ] Row count per ticker is consistent with expected trading-day count for the period (a ticker with far fewer rows than others signals a partial failure)

### Impacted components
Reuses Story 1's Spark job with a date-range parameter; no new components

### Estimated time: 2.5 hr (unchanged)

---

## Story 4: Trino query engine on the cluster

### No new Iceberg tables — this is compute/query layer, sits on top of existing tables

### Requirements
- Deploy Trino on AKS via a k8s manifest (`trino-k8s.yaml`), modeled on the existing `spark-k8s.yaml`/`airflow-k8s.yaml` pattern for consistency
- **Given your actual data volume (thousands of rows, sub-1MB), a single combined coordinator+worker node is sufficient.** Do not deploy a multi-worker Trino cluster — that's added AKS compute cost for zero query-performance benefit at this scale, and cost containment was already flagged as a real concern in Week 0. Revisit only once data volume genuinely requires horizontal scaling.
- Trino catalog configuration (`etc/catalog/iceberg.properties`) pointing at your Nessie catalog and MinIO:
```properties
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=http://nessie.finlake.svc.cluster.local:19120/api/v1
iceberg.nessie-catalog.default-warehouse-dir=s3a://finlake-warehouse/
fs.native-s3.enabled=true
s3.endpoint=http://minio.finlake.svc.cluster.local:9000
s3.path-style-access=true
s3.aws-access-key=<from your MinIO credentials secret>
s3.aws-secret-key=<from your MinIO credentials secret>
```
(Adjust service DNS names to match your actual k8s service names in `minIO.yaml`/`nessie.yaml` if they differ.)

### Acceptance criteria
- [ ] Trino pod(s) running and healthy on AKS
- [ ] `SHOW SCHEMAS FROM iceberg` lists `finlake_bronze`
- [ ] `SHOW TABLES FROM iceberg.finlake_bronze` lists both `raw_stock_prices` and `dim_tickers`
- [ ] `SELECT count(*) FROM iceberg.finlake_bronze.raw_stock_prices` returns a real, non-zero count matching what Spark sees

### Impacted components
New `trino-k8s.yaml`, new catalog properties file, no changes to existing Iceberg tables

### Estimated time: 2.5 hr (unchanged)

---

## Story 5: DBeaver connection

### No new tables — client-side configuration only

### Requirements
- Install Trino JDBC driver in DBeaver
- Connection: host = Trino coordinator's k8s service/ingress address, port 8080 (default Trino port, confirm against your `trino-k8s.yaml` service definition), catalog = `iceberg`, schema = `finlake_bronze`
- If Trino isn't exposed via ingress yet, use `kubectl port-forward svc/trino 8080:8080 -n finlake` for local access during development — don't spend Week 1 time building a public ingress for a tool only you use right now

### Acceptance criteria
- [ ] DBeaver connects successfully
- [ ] `SELECT * FROM iceberg.finlake_bronze.raw_stock_prices LIMIT 100` returns real rows in DBeaver's result grid
- [ ] A join query between `raw_stock_prices` and `dim_tickers` works from DBeaver, proving the whole path (DBeaver → Trino → Nessie → Iceberg → MinIO) end to end

### Impacted components
DBeaver local config only

### Estimated time: 1 hr (unchanged)

---

## Sprint 1 total: ~11 hr (schema design detail added ~30 min to Story 1; total otherwise matches prior estimate)

## One open decision before you start Story 1
`dim_tickers` wasn't in your original story list — I added it because Story 1 can't function without a ticker list somewhere, and putting it in Iceberg (rather than just an Airflow Variable) means it's queryable via Trino/DBeaver too, which directly serves Story 4-5's goal. If you'd rather keep the ticker list purely as an Airflow Variable and skip the `dim_tickers` table, say so — it removes about 30-45 min from Story 1 but means DBeaver users can only see raw ticker symbols, not company names, until Week 2's sector mapping work.
