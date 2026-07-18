# Where to start, what "done" looks like at each step

- MinIO + Nessie + Postgres — deploy all three, confirm Spark can write one test Iceberg table through Nessie to MinIO from a spark-submit job. This is the foundation everything else reads/writes through; nothing else is worth building until this round-trip works.
- Bronze batch ingest — new Airflow DAG, yfinance pull → PySpark write to Iceberg Bronze (raw_stock_prices). Verify with Iceberg time-travel query, not just "the DAG went green."
- dbt Silver — dbt-spark profile pointed at Nessie, one cleaning model, dbt test passing.
- dbt Gold — aggregation models (daily_returns, volatility), partitioned by date+sector.
- Great Expectations gate — Bronze and Silver suites, wired into the Airflow DAG so a DQ failure actually halts downstream tasks (this is the specific thing interviewers probe — make sure it's real, not decorative).
- Kafka streaming — Strimzi operator, tick-simulator producer, Spark Structured Streaming consumer into Bronze, plus the batch-vs-stream reconciliation DAG the PDF describes (that idea is sound, keep it).
- Superset — three dashboards against Gold, exported JSON committed to repo for reproducibility.
MLflow — forecasting model on Gold data, tracked and registered. Genuinely last — it's the only layer with no downstream dependents.

# End product:
A finlake k8s namespace (portable minikube → cloud, since nothing above is Compose-specific) where Airflow is the single control plane triggering Spark Jobs, gated by GX, transformed by dbt, cataloged by Nessie, stored in MinIO, and surfaced in Superset — plus a Strimzi-managed streaming path converging on the same Bronze layer. Every service is its own image, built by the same GHCR matrix pattern you already have, deployed by its own k8s manifest pinned to a commit SHA — no step where this description diverges from what you're already doing, just more services following the same shape.

