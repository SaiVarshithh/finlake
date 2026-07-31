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



## Trino Setup


1. helm repo add trino https://trinodb.github.io/charts
2. helm repo update
3. helm install finlake-trino trino/trino -n finlake -f trino-values.yaml
4. (_If Upgrade_) helm upgrade finlake-trino trino/trino -n finlake -f trino-values.yaml --wait
5. [trino-values](k8s\trino-values.yaml)


## [Important] Local DNS + Ingress setup
- This method can be useful to setup ingress controller for any service running in EKS or AKS or minikube, instead of frequent port-forwarding.
- **Example**: Trino:
    - [trino-ingress](k8s\trino-ingress.yaml)

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: trino-ingress
  namespace: finlake
spec:
  ingressClassName: nginx
  rules:
  - host: trino.finlake.local
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: finlake-trino-trino
            port:
              number: 8080
```
- where `finlake-trino-trino` --> svc name of trino
- `trino.finlake.local` --> host name (needs to be added to local DNS)
- kubectl get ingress :- 
```md
NAME                CLASS       HOSTS                       ADDRESS             PORTS   AGE
trino-ingress       nginx       trino.finlake.local         4.237.10.150        80      28m
```
- ` ADDRESS` --> IP address (Public IP) where Ingress controller is installed. (assigned by Cloud Provider - Azure in my case)<br>
**Note** :- The Address '4.237.10.150' is specific to my cloud provider and will be different for you. You'll need to replace it with your actual IP address.<br>
- DNS: Open Notepad in Admin mode and open `hosts` at `C:\Windows\System32\drivers\etc\hosts`. Add the following line & save and close `hosts` file:
    ```
    4.237.10.150    trino.finlake.local
    ```

- Now we can able to connect with DBeaver or browser at `http://trino.finlake.local`



## DNS:

| DNS | Username | Password |
|-----|----------|----------|
| Trino: http://trino.4.237.10.150.nip.io/ | admin | - |
| Airflow: http://airflow.4.237.10.150.nip.io/ | admin | exec into pod and cat standalone-password.txt |
| MinIO: http://minio.4.237.10.150.nip.io/ | minioadmin | minioadmin |
| Nessie: http://nessie.4.237.10.150.nip.io/ | admin | - |