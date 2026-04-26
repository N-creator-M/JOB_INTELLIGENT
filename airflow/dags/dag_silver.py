from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "job_intelligent",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_silver_transform",
    description="Nettoyage et enrichissement NLP",
    schedule="0 8 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["silver"],
) as dag:

    transform = BashOperator(
        task_id="transform_all",
        bash_command="python /app/silver/entrypoint.py",
    )