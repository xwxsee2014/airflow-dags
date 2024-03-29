from __future__ import print_function
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import DAG
import common_config as common_config
import time

default_params = {"start_number": 0, "end_number": 100}

def sleep_and_count_with_params(start_number, end_number):
    for i in range(int(end_number)):
      if i >= int(start_number):
        print(f"count update to: {i}")
      else:
        print(f"wait for starting: {i}")
      time.sleep(1)

# Before: define the global basic DAG
run_with_params_dag = DAG(
    dag_id='run_with_params',
    schedule_interval=None,
    max_active_runs=1,
    start_date=common_config.dag_start_date(),
    params=default_params,
    concurrency=1)

start_task = DummyOperator(task_id='start_task', retries=3, dag=run_with_params_dag)
python_task = PythonOperator(task_id='python_task', python_callable=sleep_and_count_with_params, 
                             op_kwargs={
                              "start_number": "{{ params.start_number }}",
                              "end_number": "{{ params.end_number }}"
                            }, dag=run_with_params_dag)

start_task >> python_task
