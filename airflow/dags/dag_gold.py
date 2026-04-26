from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "job_intelligent",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_gold_aggregate",
    description="Calcul des KPIs",
    schedule="0 10 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["gold"],
) as dag:

    aggregate = BashOperator(
        task_id="aggregate_all",
        bash_command="python /app/gold/entrypoint.py",
    )