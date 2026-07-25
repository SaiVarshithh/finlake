# FinLake — Sprint Board (Next 6 Weeks)

**Status entering Week 1:** Foundation confirmed working — DAG runs end-to-end, Spark writes to Iceberg via Nessie. Starting fresh from here.
**Sprint length:** 1 week = 1 sprint (Friday + Saturday full days)
**Target:** ~40% overall project movement across these 6 weeks

---

## Week 1 — Real Stock Data Ingestion + Query Access

**Sprint Goal:** Replace overwrite-based test data with real, historical NSE/BSE stock data landing correctly in Iceberg, and make it queryable via a real SQL client — not just Spark shell.

| # | User Story | Acceptance Criteria | Impacted Components | Est. Time |
|---|---|---|---|---|
| 1 | As a data engineer, I want daily NSE/BSE OHLCV data pulled via yfinance and written to Iceberg, **appended and partitioned by date**, so historical data is never lost on re-run | - Job writes to `nessie.finlake_bronze.raw_stock_prices`<br>- Re-running the DAG twice in one day does not duplicate or destroy prior partitions<br>- At least 20 tickers land successfully | Spark job (new), Iceberg Bronze schema | 3 hr |
| 2 | As a data engineer, I want a scheduled Airflow DAG for this ingest so it runs without manual triggering | - DAG runs on a daily schedule<br>- Ticker list is configurable via Airflow Variable, not hardcoded | New Airflow DAG | 2 hr |
| 3 | As a data engineer, I want 6-12 months of historical data backfilled so downstream analytics have enough history to be meaningful | - `raw_stock_prices` contains 6+ months of data per ticker<br>- Confirmed via row count per ticker, no gaps in trading days | Backfill script | 2.5 hr |
| 4 | As a data engineer, I want a Trino query engine deployed on the cluster, pointed at the Nessie/Iceberg catalog, so tables are queryable via standard SQL from outside Spark | - Trino pod running on AKS<br>- Trino catalog config successfully lists `finlake_bronze` schema and tables | New Trino deployment (k8s) | 2.5 hr |
| 5 | As a data engineer, I want to connect DBeaver to Trino so I (and anyone reviewing the project) can browse and query Iceberg tables with a normal SQL client | - DBeaver connects to Trino successfully<br>- A `SELECT * FROM finlake_bronze.raw_stock_prices LIMIT 100` runs and returns real rows | DBeaver connection config | 1 hr |

**Week 1 total: ~11 hr**

---

## Week 2 — Silver Layer: Clean, Trusted Data

**Sprint Goal:** Raw prices become deduplicated, validated, analysis-ready data via dbt.

| # | User Story | Acceptance Criteria | Impacted Components | Est. Time |
|---|---|---|---|---|
| 1 | As a data engineer, I want a dbt project connected to the Iceberg catalog (via Trino or Spark) so I can start transforming Bronze data | - `dbt debug` passes<br>- dbt can query `raw_stock_prices` as a source | dbt project init | 2 hr |
| 2 | As an analyst, I want a clean, deduplicated `clean_prices` table so I'm not working with raw duplicates or bad types | - No duplicate (ticker, date) rows<br>- Correct types on price/volume columns<br>- Nulls handled explicitly, not silently dropped | New Silver model | 2.5 hr |
| 3 | As a data engineer, I want automated tests on `clean_prices` so bad data is caught before it reaches Gold | - `dbt test` passes with zero failures<br>- Tests cover not-null, uniqueness, and a sane price-range check | dbt tests | 1.5 hr |
| 4 | As an analyst, I want a ticker-to-sector mapping loaded so I can later aggregate performance by sector | - Sector mapping seed loaded via `dbt seed`<br>- Joins cleanly against `clean_prices` on ticker | New seed file | 1.5 hr |
| 5 | As a stakeholder, I want dbt lineage docs generated so the Bronze→Silver flow is visible without reading code | - `dbt docs generate` succeeds<br>- Lineage graph shows Bronze source → Silver model | dbt docs | 1 hr |

**Week 2 total: ~8.5 hr**

---

## Week 3 — Gold Layer: Business-Ready Analytics

**Sprint Goal:** The numbers a risk/investment analyst would actually want to look at.

| # | User Story | Acceptance Criteria | Impacted Components | Est. Time |
|---|---|---|---|---|
| 1 | As an analyst, I want daily returns computed per ticker so I can see day-over-day performance | - `daily_returns` model produces correct % change per ticker/day<br>- Spot-checked against a manual calculation for 1 ticker | New Gold model | 1.5 hr |
| 2 | As an analyst, I want rolling volatility (7d/30d) so I can assess risk, not just returns | - Rolling stddev computed correctly over both windows<br>- Values visibly spike on a known volatile date in the backfilled range | New Gold model | 1.5 hr |
| 3 | As an analyst, I want 20d/50d moving averages so I can identify trend signals | - Moving averages match manual spot-check | New Gold model | 1 hr |
| 4 | As an analyst, I want sector-level performance aggregation so I can compare sectors, not just individual stocks | - Aggregation joins cleanly against sector seed<br>- Output grouped correctly by sector and date | New Gold model | 1.5 hr |
| 5 | As a data engineer, I want the full Bronze→Silver→Gold flow orchestrated as one DAG trigger so the pipeline isn't three manual steps | - Single DAG trigger runs ingest → dbt run → Gold models in sequence<br>- Failure in an earlier stage blocks the later stages | Airflow DAG extension | 2 hr |

**Week 3 total: ~7.5 hr**

---

## Week 4 — Data Quality Gate (Great Expectations)

**Sprint Goal:** Bad data gets caught and the pipeline actually stops — not just logs a warning.

| # | User Story | Acceptance Criteria | Impacted Components | Est. Time |
|---|---|---|---|---|
| 1 | As a data engineer, I want Great Expectations connected to Bronze data so I can validate it automatically | - GX datasource successfully connects to `raw_stock_prices` | GX init | 1.5 hr |
| 2 | As a data engineer, I want a Bronze validation suite so schema and volume problems are caught at the source | - Suite checks null rate, schema match, and daily row-count range<br>- Passes on current clean data | Bronze expectation suite | 1.5 hr |
| 3 | As a data engineer, I want a Silver validation suite so business-rule violations (e.g. negative prices) are caught | - Suite checks no negative prices, no future-dated rows<br>- Passes on current `clean_prices` | Silver expectation suite | 1 hr |
| 4 | As a data engineer, I want the pipeline to actually halt when a GX check fails, so bad data never silently reaches Gold | - A deliberately broken test row (negative price) causes the DAG to fail and downstream tasks to skip<br>- Confirmed via Airflow UI, not assumed | Airflow DAG + GX integration | 2.5 hr |
| 5 | As a data engineer, I want DQ check results stored somewhere queryable so I can track data quality over time | - Checkpoint results written to MinIO as structured JSON per run | GX checkpoint config | 1 hr |

**Week 4 total: ~7.5 hr**

---

## Week 5 — Real-Time Layer (Kafka Streaming)

**Sprint Goal:** Move beyond daily batch — intraday price ticks flow through Kafka into Iceberg.

| # | User Story | Acceptance Criteria | Impacted Components | Est. Time |
|---|---|---|---|---|
| 1 | As a data engineer, I want a Kafka broker running on the cluster so I have a streaming backbone | - Kafka pod(s) running on AKS<br>- A test topic can be created and messages produced/consumed | New Kafka k8s deployment | 2.5 hr |
| 2 | As a data engineer, I want a producer publishing intraday price ticks to Kafka so real-time data has a source | - Producer publishes ticks for at least 5 tickers at a regular interval to a topic | New Kafka producer | 2.5 hr |
| 3 | As a data engineer, I want a Spark Structured Streaming job consuming ticks and writing to Iceberg so streaming data lands in the same lake as batch data | - Streaming job consumes from Kafka and writes to a `raw_stock_prices_streaming` Iceberg table<br>- Confirmed rows appear within expected latency | New Spark Streaming job | 3 hr |
| 4 | As a data engineer, I want streaming and batch Bronze data reconciled so I don't have two disconnected sources of truth | - A documented (even if manual for now) approach for how streaming ticks relate to the daily batch table | Design note / reconciliation logic | 1.5 hr |
| 5 | As an analyst, I want to query streaming data via Trino/DBeaver just like batch data | - `SELECT` against `raw_stock_prices_streaming` returns real rows in DBeaver | Trino catalog config | 1 hr |

**Week 5 total: ~10.5 hr**

---

## Week 6 — Visualization + ML Baseline

**Sprint Goal:** The project becomes demoable — dashboards a stakeholder can look at, and a first ML model logged properly.

| # | User Story | Acceptance Criteria | Impacted Components | Est. Time |
|---|---|---|---|---|
| 1 | As a stakeholder, I want Superset connected to Trino so I can explore Gold data visually | - Superset connects to Trino catalog successfully | New Superset deployment | 2 hr |
| 2 | As a stakeholder, I want a returns/volatility dashboard so I can see performance at a glance | - Dashboard shows daily returns and rolling volatility for at least 5 tickers | Superset dashboard | 2 hr |
| 3 | As a stakeholder, I want a sector performance dashboard so I can compare sectors visually | - Dashboard shows sector-level aggregates from the Gold layer | Superset dashboard | 1.5 hr |
| 4 | As a data scientist, I want an MLflow tracking server running so model experiments are logged, not lost | - MLflow server deployed and reachable<br>- A test run logs successfully | New MLflow deployment | 2 hr |
| 5 | As a data scientist, I want a baseline model (e.g. next-day return direction classifier) trained and logged to MLflow so there's a first real ML artifact in the project | - Model trains on Gold data<br>- Metrics and model artifact logged to MLflow, retrievable | Baseline ML script | 2.5 hr |

**Week 6 total: ~10 hr**

---

## Reality check on the 40% target

30 stories across 6 weeks, ~55 total hours, covers: real Bronze ingest, a query engine, full Silver/Gold, a real DQ gate, streaming, dashboards, and a first ML model. That's a legitimate, demoable slice of the 12-month PDF scope — **not because 30 stories happens to equal 40% of some story count, but because it covers one full vertical slice of the architecture end-to-end** (ingest → clean → analytics → quality → streaming → visualization → ML). If any single week's stories run long — Week 1 story 4 (Trino) and Week 5 story 3 (Streaming) are the two most likely to overrun — cut the Week 6 sector dashboard (Week 6 #3) first; it's the lowest-dependency story in the whole board.
