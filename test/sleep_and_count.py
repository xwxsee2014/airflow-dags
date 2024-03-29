from __future__ import print_function
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import DAG
from EmbulkWSPlugin import EmbulkWSOperator
import common_config as common_config
import time

def sleep_and_count():
    for i in range(10):
      print(f"count update to: {i}")
      time.sleep(1)

def sleep_and_sum():
    sum = 0
    for i in range(10):
      sum += i
      print(f"sum update to: {sum}")
      time.sleep(1)

def sleep_and_multiply():
    product = 0
    for i in range(10):
      product *= i
      print(f"product update to: {product}")
      time.sleep(1)

def sleep_and_substract():
    substract = 10
    for i in range(10):
      substract -= i
      print(f"substract update to: {substract}")
      time.sleep(1)

def sleep_and_divide():
    divide = 100
    for i in range(10):
      if i > 0:
        divide /= i
        print(f"divide update to: {divide}")
        time.sleep(1)

# Before: define the global basic DAG
sleep_and_count_dag = DAG(dag_id='sleep_and_count',
                          default_args=common_config.dag_default_args(),
                          schedule_interval='@once',
                          max_active_runs=1,
                          start_date=common_config.dag_start_date(reduce_days=0),
                          concurrency=6)

start_task = DummyOperator(task_id='start_task', retries=3, dag=sleep_and_count_dag)
python_task = PythonOperator(task_id='python_task', python_callable=sleep_and_count, dag=sleep_and_count_dag)
python_task_sum = PythonOperator(task_id='python_task_sum', python_callable=sleep_and_sum, dag=sleep_and_count_dag)
python_task_multiply = PythonOperator(task_id='python_task_multiply', python_callable=sleep_and_multiply, dag=sleep_and_count_dag)
# python_task_substract = PythonOperator(task_id='python_task_substract', python_callable=sleep_and_substract, dag=sleep_and_count_dag)
python_task_divide = PythonOperator(task_id='python_task_divide', python_callable=sleep_and_divide, dag=sleep_and_count_dag)
finish_task = DummyOperator(task_id='finish_task', retries=3, dag=sleep_and_count_dag)

start_task >> python_task >> finish_task
start_task >> python_task_sum >> finish_task
start_task >> python_task_multiply >> finish_task
start_task.set_downstream(python_task_divide)
finish_task.set_upstream(python_task_divide)
embulk_task = {}
for i in range(4):
  embulk_task[i] = EmbulkWSOperator(task_id="test_task_"+str(i), controller='run', parameters={}, dag=sleep_and_count_dag, pool="new_pool", config_path="/home/airflow/redmodo.conf")
  start_task.set_downstream(embulk_task[i])
  finish_task.set_upstream(embulk_task[i])
# start_task >> python_task_divide >> finish_task
# start_task >> python_task_substract >> finish_task
