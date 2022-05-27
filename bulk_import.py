import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='bulk_import',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    upload_file = BashOperator(
        task_id='upload_file',
        bash_command="cp -r /tmp/data/src/. ~/ && chmod +x ~/bcp_import && hadoop dfs -cat /user/smartadmin/data/data.csv|~/bcp_import ",
        )
    
    upload_file 
   
  
