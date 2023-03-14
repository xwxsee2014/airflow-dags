from __future__ import print_function
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import DAG
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
sleep_and_count_dag = DAG(
    dag_id='sleep_and_count',
    schedule_interval="30 0 * * *",
    max_active_runs=1,
    start_date=common_config.dag_start_date(reduce_days=5),
    concurrency=3)

start_task = DummyOperator(task_id='start_task', retries=3, dag=sleep_and_count_dag)
python_task = PythonOperator(task_id='python_task', python_callable=sleep_and_count, dag=sleep_and_count_dag)
python_task_sum = PythonOperator(task_id='python_task_sum', python_callable=sleep_and_sum, dag=sleep_and_count_dag)
python_task_multiply = PythonOperator(task_id='python_task_multiply', python_callable=sleep_and_multiply, dag=sleep_and_count_dag)
python_task_substract = PythonOperator(task_id='python_task_substract', python_callable=sleep_and_substract, dag=sleep_and_count_dag)
python_task_divide = PythonOperator(task_id='python_task_divide', python_callable=sleep_and_divide, dag=sleep_and_count_dag)

# start_task >> python_task
start_task >> python_task >> python_task_sum
start_task >> python_task_multiply

# python_task_substract
start_task >> python_task_substract >> python_task_divide
