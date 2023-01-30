from __future__ import print_function
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import DAG
import common_config as common_config
import time

def sleep_and_count():
    for i in range(10):
      print(f"count update to: {i}")
      time.sleep(15)

# Before: define the global basic DAG
sleep_and_count_dag = DAG(
    dag_id='sleep_and_count',
    schedule_interval="*/5 * * * *",
    max_active_runs=1,
    start_date=common_config.dag_start_date(),
    concurrency=3)

start_task = EmptyOperator(task_id='start_task', retries=3, dag=sleep_and_count_dag)
python_task = PythonOperator(task_id='python_task', python_callable=sleep_and_count, dag=sleep_and_count_dag)

start_task >> python_task
