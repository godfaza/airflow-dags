import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='dynamic_tasks',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

  start_op = BashOperator(
        task_id='start_op',
        bash_command='echo XSDSDSSD ',
            )    
  a = []
  for i in range(0,10):
    a.append(DummyOperator(
        task_id='Component'+str(i),
        dag=dag))
    if i == 0 :
        start_op >> a[i]
    if i not in [0]: 
        a[i-1] >> a[i]
   
  
