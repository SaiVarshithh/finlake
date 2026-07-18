FROM apache/airflow:2.9.3-python3.11

# ------------------------------------------------------------------------------
# Install extra Python dependencies
# Constraint file pins versions compatible with Airflow 2.9.3's own
# dependency graph — required by Airflow's own install guidance, not
# optional. See requirements.txt for why versions aren't hard-pinned there.
# ------------------------------------------------------------------------------
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

# ------------------------------------------------------------------------------
# DAGs
# ------------------------------------------------------------------------------
COPY dags/ /opt/airflow/dags/
