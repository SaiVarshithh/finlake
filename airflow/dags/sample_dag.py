"""
==============================================================================
FinLake — Sample DAG
==============================================================================
Minimal DAG whose only job is to prove the deployment works end-to-end:
the scheduler picks it up, it shows up in the UI, you can trigger it
manually, and the tasks run and succeed.

schedule=None on purpose — trigger it yourself from the UI or CLI:
  kubectl exec -it deploy/finlake-airflow -n finlake -- \
    airflow dags trigger finlake_sample_dag

Once this is confirmed working, replace/extend this with real pipeline
logic (e.g. calling out to the Spark job via kubectl, as the previous
finlake_spark_pipeline DAG sketched) — but get this green first.
==============================================================================
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def say_hello(**context) -> None:
    print(f"Hello from FinLake Airflow! run_id={context['run_id']}")


with DAG(
    dag_id="finlake_sample_dag",
    description="Minimal sample DAG to verify the Airflow deployment works",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finlake", "sample"],
    default_args={"owner": "finlake", "retries": 0},
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting FinLake sample DAG'",
    )

    hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Done'",
    )

    start >> hello >> end
