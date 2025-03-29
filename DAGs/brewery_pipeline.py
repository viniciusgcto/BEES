from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.transform_data import fetch_and_create_dataframe

with DAG(
    dag_id='brewery_pipeline',
    schedule_interval="0 7 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    extract_transform_load = PythonOperator(
        task_id='fetch_and_create_dataframe',
        python_callable=fetch_and_create_dataframe,
    )
