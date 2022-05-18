import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='object_storage_mount',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    check_mount = BashOperator(
        task_id='check_mount',
        bash_command="echo geesfs write tests >  /tmp/data/out.txt",
   
    )
    
    java_check = BashOperator(
        task_id='java_check',
        bash_command="echo ABCDEFGxyz|hadoop fs -put - hdfs://rc1b-dataproc-m-3iu6zt2tusazxrxi.mdb.yandexcloud.net/user/smartadmin/data/out.txt",
   
    )

    check_mount >> java_check
   
  
