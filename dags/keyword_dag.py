from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PYTHON = "/home/aswinsairam/airflow_env/bin/python3"

default_args = {
    "owner":           "airflow",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="keyword_analytics_pipeline",
    default_args=default_args,
    description="Fetches keyword trends daily and loads to Delta Lake",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["keyword", "marketing", "pyspark"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_keyword_data",
        bash_command=f'{PYTHON} /home/aswinsairam/ingest_linux.py',
    )

    transform = BashOperator(
        task_id="transform_keyword_data",
        bash_command=f'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 && {PYTHON} /home/aswinsairam/transform_linux.py',
    )

    gold = BashOperator(
        task_id="load_gold_table",
        bash_command=f'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 && {PYTHON} /home/aswinsairam/gold_linux.py',
    )

    ingest >> transform >> gold
