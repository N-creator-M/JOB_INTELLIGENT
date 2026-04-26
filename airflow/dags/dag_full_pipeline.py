from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "job_intelligent",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_full_pipeline",
    description="Pipeline complet Bronze → Silver → Gold",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pipeline"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command="python /app/bronze/entrypoint.py --source all",
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command="python /app/silver/entrypoint.py --step all",
    )

    gold = BashOperator(
        task_id="gold_aggregate",
        bash_command="python /app/gold/entrypoint.py --step all",
    )

    bronze >> silver >> gold