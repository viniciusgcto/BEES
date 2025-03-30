from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.transform_data import fetch_and_create_dataframe
from airflow.models import TaskInstance
from airflow.utils.email import send_email

def task_failure_alert(context):
    task_instance: TaskInstance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url

    html_content = (
        f"<p>Task {task_id} in DAG {dag_id} failed.</p>"
        f"<p>Execution date: {execution_date}</p>"
        f"<p>Logs: <a href=\"{log_url}\">View logs</a></p>"
    )

    send_email(
        to="monitoramento@bees.com",
        subject=f"Falha na execução da tarefa no Airflow: {dag_id}.{task_id}",
        html_content=html_content,
    )

with DAG(
    dag_id='brewery_pipeline',
    schedule_interval="0 7 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    extract_transform_load = PythonOperator(
        task_id='fetch_and_create_dataframe',
        python_callable=fetch_and_create_dataframe,
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=task_failure_alert,
        email="monitoramento@bees.com",
        email_on_failure=True,
    )
