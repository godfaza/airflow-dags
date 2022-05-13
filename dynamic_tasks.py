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

  download_schema = BashOperator(
        task_id='download_schema',
        bash_command="cp -r /opt/airflow/logs/src/. ~/ && chmod +x ~/download_schema.sh && ~/download_schema.sh ",
            )  
  start_op = BashOperator(
        task_id='upload_schema',
        bash_command='cp -r /opt/airflow/logs/src/. ~/ && ',
            ) 
    
  
  print(lines)
  a = []
  for i in range(0,10):
    a.append(DummyOperator(
        task_id='Component'+str(i),
        dag=dag))
    if i == 0 :
       download_schema >> start_op >> a[i]
    if i not in [0]: 
        a[i-1] >> a[i]
   
  
