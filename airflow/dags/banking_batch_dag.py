from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "nhutdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
with DAG(
    dag_id="banking_batch_enrichment",
    description="Batch Layer: dbt run + dbt test tren BigQuery",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["banking", "dbt", "batch-layer"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --no-partial-parse",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --no-partial-parse",
    )

    dbt_run >> dbt_test
