from __future__ import print_function
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import DAG
import common_config as common_config
import time

def sleep_and_count():
    for i in range(5):
      print(f"count2 to: {i}")
      time.sleep(30)

# Before: define the global basic DAG
sleep_and_count2_dag = DAG(
    dag_id='sleep_and_count2',
    schedule_interval="*/10 * * * *",
    max_active_runs=1,
    start_date=common_config.dag_start_date(),
    concurrency=3)

start_task = EmptyOperator(task_id='start_task2', retries=3, dag=sleep_and_count2_dag)
python_task = PythonOperator(task_id='python_task2', python_callable=sleep_and_count, dag=sleep_and_count2_dag)

start_task >> python_task
