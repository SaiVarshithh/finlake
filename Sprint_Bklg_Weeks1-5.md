# FinLake — 5-Week Sprint Backlog

**Basis:** Live repo scan (E:\Fresh-Learnings-2026\Data-Eng-Projects\finlake) + FinLake_Project_Architecture.pdf + NOTES.md, cross-checked against your confirmed answers below.

## Ground truth going into Week 1

| Fact | Status | Why it matters |
|---|---|---|
| Compose vs K8s | **Pivoted to Kubernetes.** `docker-compose.yml` is 0 bytes. Infra (`airflow-k8s.yaml`, `spark-k8s.yaml`, `k8s/minIO.yaml`, `nessie.yaml`, `posgresql.yaml`, `ingress/airflow-ingress.yaml`) is all k8s-native. | The PDF's plan and timeline assume Compose. Every "Month N" mapping below is re-sequenced for k8s reality, not copy-pasted from the PDF. |
| Cluster location | **AKS — confirmed active spend.** | You are paying Azure while the one thing NOTES.md calls a hard gate — the Spark→Nessie→MinIO round trip — has never completed successfully. That is the actual state of the project right now: **cost accruing against unproven infrastructure.** Week 1 exists to close that gap, not to add features. |
| Foundation round trip | **Triggered, failed / unconfirmed.** `finlake_iceberg_spark_ingest` DAG exists, submits a K8s Job running `iceberg_transactions_job.py` (100k synthetic transaction rows → `nessie.finlake_bronze.transactions_test`), but has never been verified end-to-end. | Per your own NOTES.md: *"nothing else is worth building until this round-trip works."* I'm holding you to that rule even though it's less exciting than starting Bronze ingest. |
| Data shape already built | The one working Spark job writes **synthetic BFSI transaction data** (account/customer/merchant/risk_score) — not the PDF's yfinance OHLCV stock data. | [Likely] intentional given your day job is BFSI risk analytics, not a mistake — but it means "Bronze layer" in your repo currently means two different things (test transactions vs. planned stock prices). Week 2 below builds the *actual* stock Bronze layer alongside the test one; it does not replace it. |
| Time budget | **~7-9 hrs/week, Friday night + Saturday only.** | 5 stories/week as requested = ~1.5-2 hrs/story average. Week 1 is diagnosis-heavy and will run tighter than that; weeks 3-5 have more headroom. If Week 1 stories 1-2 blow past 3 hours combined, story 5 (ADR write-up) is the one to cut, not the fix itself. |

## Disagreement, stated plainly

You asked for stories mapped straight across 5 weeks of new capability. I'm not doing that for Week 1. Building Bronze/Silver/Gold on top of an unconfirmed foundation means every subsequent DAG failure this month has two possible causes — new code or the original unfixed infra bug — and you won't be able to tell which without redoing this diagnosis anyway, at a worse time, under more pressure. The risk in going straight to feature stories: you spend Week 2-3 debugging what looks like a dbt or yfinance problem but is actually the same unresolved Nessie/S3/RBAC issue from Week 1, and you're now debugging it across three layers instead of one.

---

## Week 1 — Foundation Verification & Cost Containment

*Goal: the round trip NOTES.md calls non-negotiable actually passes, and AKS stops bleeding money for nothing.*

| # | Title | Functional Requirement | Description | Est. Time | Expected Outcome | Procedure | Impacted Components |
|---|---|---|---|---|---|---|---|
| 1 | Diagnose the failed ingest run | System must surface the exact failure point of `finlake_iceberg_spark_ingest` | Pull Airflow task logs for both `ensure_minio_warehouse_bucket` and `submit_spark_iceberg_ingest`; pull K8s pod logs for the Spark driver pod via `kubectl logs -n finlake -l app=finlake-spark,spark-role=driver`. Classify the failure: MinIO bucket creation, Spark driver scheduling (RBAC/service account), Nessie catalog connectivity, or S3 path-style access to MinIO. | 1.5 hr | A written one-line diagnosis of the actual failure cause (not "it failed") | `kubectl get jobs,pods -n finlake -l spark-pipeline=iceberg-transactions` → `kubectl describe pod <pod>` for the failing one → `kubectl logs` on driver and any init container → cross-check against `spark-rbac.yaml` permissions and `nessie.yaml` service config | Airflow DAG, `spark-rbac.yaml`, `nessie.yaml`, `minIO.yaml` |
| 2 | Fix and re-run to first green pass | DAG must complete both tasks successfully once | Apply the fix identified in #1 (likely RBAC scope, image tag drift, or Nessie URI/service DNS mismatch inside the cluster). Rebuild/push image via existing GHCR Action if code changed. Re-trigger DAG manually. | 2 hr | `finlake_iceberg_spark_ingest` shows both tasks green in Airflow UI on a real run, not a manual override | Fix root cause → `git push` (triggers `.github/workflows/docker.yml`) if image changed → `kubectl rollout restart deploy/finlake-airflow -n finlake` → trigger DAG from Airflow UI → watch `kubectl get pods -n finlake -w` | `finlake_iceberg_spark_dag.py`, `iceberg_transactions_job.py`, GHCR image build |
| 3 | Verify with an actual query, not a green DAG | Bronze table must be queryable with correct row count and support time travel | Confirm `nessie.finlake_bronze.transactions_test` has ≥100,000 rows via `spark.table(...).count()`, then run an Iceberg time-travel query (`SELECT * FROM table VERSION AS OF <snapshot>`) to confirm snapshot history exists | 45 min | A saved query output showing row count + a successful `AS OF` query — this is your interview-ready proof, not just "the DAG went green" | Spark shell or a scratch job against the Nessie catalog; `SELECT * FROM nessie.finlake_bronze.transactions_test.snapshots` to list snapshot IDs, then re-query `AS OF VERSION <id>` | Iceberg table, Nessie catalog |
| 4 | Contain AKS spend between sessions | Cluster compute must not run (and bill) when you are not actively working | Identify the node pool backing your AKS cluster and either (a) script `az aks nodepool scale --node-count 0` for teardown after each session, or (b) confirm cluster autoscaler is already scaling to a genuine zero-cost floor. Document the exact stop/start commands in README. | 1 hr | A tested one-command stop and one-command start procedure, run once each to confirm they work | `az aks nodepool list` → identify pool name → `az aks nodepool scale --resource-group <rg> --cluster-name <cluster> --name <pool> --node-count 0` to stop; scale back up before next session; time how long startup takes so you can budget it into next Saturday | AKS cluster config, README |
| 5 | Document the Compose→AKS pivot as an ADR | The architectural deviation from the PDF plan must be captured while the reasoning is still fresh | Write `docs/architecture/decisions/ADR-000-k8s-over-compose.md` covering: why you moved off Compose, why AKS specifically vs. local minikube, and the cost tradeoff you're accepting | 45 min | One committed ADR file, following the same format as the PDF's ADR-001 through 005 | Copy the ADR table format from the PDF page 7-8; write Decision + Reasoning sections; commit | `docs/architecture/decisions/` |

**Week 1 total: ~6 hrs.** Leaves slack in your 7-9 hr budget for #1 or #2 running long, which is likely on a first real debug pass.

---

## Week 2 — Real Bronze Layer (yfinance, replacing the placeholder dataset)

*Goal: the actual PDF-spec Bronze layer — daily NSE/BSE OHLCV — exists and is verified, independent of the transactions_test table from Week 1.*

| # | Title | Functional Requirement | Description | Est. Time | Expected Outcome | Procedure | Impacted Components |
|---|---|---|---|---|---|---|---|
| 1 | Add yfinance to the Spark image | Spark job must be able to pull live market data | Add `yfinance` to `spark/requirements.txt`; rebuild image via GHCR Action | 45 min | Image builds clean with yfinance importable inside the container | Edit `spark/requirements.txt` → commit/push → confirm Action succeeds in GitHub | `spark/requirements.txt`, GHCR image |
| 2 | Write the yfinance pull + Bronze writer job | System must pull previous-day OHLCV for 20 Nifty-50 tickers and write to Iceberg | New `spark/jobs/bronze_stock_writer.py`: hardcode or env-var the 20-ticker list, pull via `yfinance.download()`, write to `nessie.finlake_bronze.raw_stock_prices` partitioned by date | 2.5 hr | Job runs locally (`local[*]`) against a test ticker subset before touching the cluster | Write script → smoke test with `SPARK_MASTER=local[*]` and 2-3 tickers → validate schema and row shape before scaling to all 20 | New Spark job file, Bronze schema |
| 3 | New Airflow DAG for daily batch ingest | Ingest must be schedulable and parametrized, not hardcoded | New DAG `bronze_ingest_daily.py` using the K8s Job pattern from `finlake_iceberg_spark_dag.py`; ticker list pulled from an Airflow Variable, not hardcoded in code | 1.5 hr | DAG triggers manually and produces a populated `raw_stock_prices` table on first run | Model the new DAG on the existing one (reuse `create_job`/`wait_for_job` helper pattern) → set `FINLAKE_TICKERS` Airflow Variable → trigger manually | `airflow/dags/bronze_ingest_daily.py` |
| 4 | Schema enforcement + null gate on write | Bad or missing data must not silently land in Bronze | Before write: assert expected columns/types present, reject the batch if the null rate on `close` or `volume` exceeds a threshold (basic pre-GX gate — full GX comes Week 5) | 1 hr | A deliberately corrupted test run (drop a column) fails loudly instead of writing partial data | Add a schema-check function before `.writeTo()`; unit-test it against one clean and one broken sample DataFrame | Bronze writer job |
| 5 | Backfill + time-travel verification | Historical data must exist for downstream Silver/Gold work to be meaningful | Backfill 3-6 months (scoped down from the PDF's 12 months given your hours budget) by looping the pull across a date range; verify with an `AS OF` time-travel query | 1.5 hr | `raw_stock_prices` has 3-6 months of partitioned history, confirmed via time travel | Loop backfill script over date range → run ingest → `SELECT ... VERSION AS OF` to confirm partition history is intact | Bronze table partitions |

**Week 2 total: ~7.25 hrs.**

---

## Week 3 — dbt Silver Layer

*Goal: first dbt project exists, `clean_prices` model runs and tests pass against real Bronze data.*

| # | Title | Functional Requirement | Description | Est. Time | Expected Outcome | Procedure | Impacted Components |
|---|---|---|---|---|---|---|---|
| 1 | Init dbt-spark project against Nessie | dbt must be able to connect to the Nessie/Spark catalog | `pip install dbt-spark`, `dbt init` inside `/dbt`, configure `profiles.yml` to point at your Spark/Nessie endpoint | 1.5 hr | `dbt debug` passes cleanly | Install adapter → init project → configure profile with Nessie catalog + MinIO S3 endpoint creds → `dbt debug` | `dbt/`, `profiles.yml` |
| 2 | Define sources.yml for Bronze | dbt must reference Bronze Iceberg tables as declared sources | `dbt/models/staging/sources.yml` pointing at `raw_stock_prices` and (optionally) `transactions_test` | 45 min | `dbt source freshness` or a simple `select * from {{ source(...) }}` resolves correctly | Write sources.yml with table + schema refs → test resolution with a throwaway staging model | `dbt/models/staging/sources.yml` |
| 3 | Build clean_prices Silver model | Silver table must be deduped, null-handled, type-cast, and calendar-aligned | `dbt/models/silver/clean_prices.sql`: dedup on (ticker, date), cast types, handle nulls, join against a trading-calendar reference | 2 hr | `dbt run -s clean_prices` produces the table with zero unexpected nulls | Write SQL model → `dbt run -s clean_prices` → spot-check output row counts vs Bronze | New Silver model |
| 4 | Add dbt tests to clean_prices | Model must have automated correctness checks | `not_null`, `unique`, and one `accepted_values`-style custom test (e.g. sane price range) | 1 hr | `dbt test` passes with zero failures | Add tests in schema.yml under the model → `dbt test -s clean_prices` | `dbt/models/silver/schema.yml` |
| 5 | Generate + commit dbt docs | Lineage from Bronze to Silver must be visible without reading code | `dbt docs generate` → commit static site output to repo | 45 min | A committed HTML lineage graph showing Bronze → Silver flow | `dbt docs generate` → `dbt docs serve` to sanity-check locally → commit `target/` docs output or a docs-specific export | `dbt/target/`, repo docs |

**Week 3 total: ~6 hrs.**

---

## Week 4 — dbt Gold Layer

*Goal: business-ready aggregates exist and are queryable — this is what Superset will point at in Week 9-10 of the original PDF timeline, brought forward here so you have something demoable sooner.*

| # | Title | Functional Requirement | Description | Est. Time | Expected Outcome | Procedure | Impacted Components |
|---|---|---|---|---|---|---|---|
| 1 | daily_returns Gold model | Gold table must compute day-over-day returns per ticker | `dbt/models/gold/daily_returns.sql` using window functions over `clean_prices` | 1.5 hr | `dbt run -s daily_returns` produces correct % change per ticker/day | Write SQL with `LAG()` window function → `dbt run` → spot-check against a manual calculation for one ticker | New Gold model |
| 2 | volatility_7d_30d model | Gold table must compute rolling volatility windows | Rolling stddev of returns over 7-day and 30-day windows | 1.5 hr | Rolling volatility values that visibly react to a known high-volatility date in your backfilled range | Write windowed stddev SQL → `dbt run` → sanity-check against a known market-moving date | New Gold model |
| 3 | moving_avg_20_50 model | Gold table must compute 20-day and 50-day moving averages | Standard rolling average window functions | 1 hr | Values line up with a manual spot-check against raw prices | Write SQL → `dbt run` → verify | New Gold model |
| 4 | sector_performance model | Gold table must aggregate returns by sector | Requires a sector-mapping seed file (ticker → sector) loaded via `dbt seed` | 1.5 hr | Sector-level aggregates queryable, seed file committed | Create `seeds/sector_mapping.csv` → `dbt seed` → write aggregation model joining against it | New seed file + Gold model |
| 5 | Wire dbt into the Airflow DAG | Pipeline must run Bronze → Silver → Gold as one orchestrated flow, not manual `dbt run` calls | Extend `bronze_ingest_daily.py` (or add a new DAG) to trigger `dbt run` as a downstream task after Bronze ingest succeeds | 1.5 hr | One DAG trigger produces Bronze → Silver → Gold end to end | Add a `BashOperator` or `KubernetesPodOperator` step running `dbt run` after the Bronze task → set task dependency → trigger full DAG | Airflow DAG |

**Week 4 total: ~7 hrs.**

---

## Week 5 — Great Expectations Gate (start)

*Goal: the DQ gate exists and — critically, per the PDF's own interview-prep framing — actually halts the pipeline on failure. Not decorative.*

| # | Title | Functional Requirement | Description | Est. Time | Expected Outcome | Procedure | Impacted Components |
|---|---|---|---|---|---|---|---|
| 1 | GX Core install + init | GX must be able to connect to Bronze Iceberg data via Spark | `pip install great_expectations`, init project, configure a Spark-backed datasource against `raw_stock_prices` | 1.5 hr | `great_expectations datasource list` shows a working connection | Install → `great_expectations init` → configure datasource.yml for Spark | `great_expectations/` |
| 2 | bronze_suite expectation set | Bronze data must be validated for null rate, schema, and volume range | Expectations: null rate < 2% on key columns, schema match, daily row-count within expected range | 1.5 hr | Suite runs against real Bronze data and passes on clean data | Build suite interactively via GX CLI/notebook → save as `bronze_suite.json` | `great_expectations/expectations/bronze_suite.json` |
| 3 | silver_suite expectation set | Silver data must be validated for business rules | Expectations: no negative prices, no future-dated rows, deduping confirmed | 1 hr | Suite runs against `clean_prices` and passes | Build suite → save `silver_suite.json` | `great_expectations/expectations/silver_suite.json` |
| 4 | Wire GX into Airflow with a real failure test | Pipeline must halt (not just log) when a GX check fails | Add `GreatExpectationsOperator`-equivalent task to the DAG after Bronze and after Silver; **deliberately break one expectation** (e.g. inject a negative price) to prove the halt actually happens | 2 hr | A screenshot/log of the pipeline genuinely failing and downstream tasks skipping — this is one of the PDF's three "cannot fake" interview questions | Add GX check tasks to DAG → set `trigger_rule` so downstream skips on failure → run once clean, once deliberately broken → confirm skip behavior in Airflow UI | Airflow DAG, GX checkpoints |
| 5 | Store DQ results as a queryable meta-table | DQ history must be queryable, not just log lines | Write GX checkpoint JSON output to MinIO, structured so it can later become its own Iceberg meta-table (full implementation can extend past Week 5) | 1 hr | JSON checkpoint results land in a MinIO path per run | Configure checkpoint result store to write to MinIO → confirm output structure | `great_expectations/checkpoints/` |

**Week 5 total: ~7 hrs.**

---

## What's deliberately NOT in weeks 1-5

Kafka streaming, Superset dashboards, MLflow, and the remaining 4 ADRs are all downstream of Gold + GX being real and stable — building them now would mean dashboards and streaming pointed at data you haven't yet proven trustworthy. That's weeks 6+, once Week 5's GX gate is confirmed actually halting the pipeline.
