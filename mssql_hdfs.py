import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='mssql_to_hdfs_export',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    # [START howto_operator_bash]
    query_db = BashOperator(
        task_id='query_db',
        bash_command='echo 1',
    )
    
    query_db
   
  
