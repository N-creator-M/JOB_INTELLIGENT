from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "job_intelligent",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_bronze_ingestion",
    description="Scraping des offres d'emploi",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze"],
) as dag:

    scrape = BashOperator(
        task_id="scrape_all_sources",
        bash_command="python /app/bronze/entrypoint.py",
    )