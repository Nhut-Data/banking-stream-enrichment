from datetime import datetime, timedelta
from airflow.sdk import dag, task

DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "nhutdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="banking_batch_enrichment",
    description="Batch Layer: dbt run + dbt test tren BigQuery",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["banking", "dbt", "batch-layer"],
)
def banking_batch_enrichment():

    @task.bash
    def dbt_run() -> str:
        return f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --no-partial-parse"

    @task.bash
    def dbt_test() -> str:
        return f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --no-partial-parse"

    dbt_run() >> dbt_test()


banking_batch_enrichment()