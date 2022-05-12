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
    create_hdfs_config = BashOperator(
        task_id=create_hdfs_config',
        bash_command="""echo [global]
        default.alias = dev

        [dev.alias]
          url = http://rc1b-dataproc-m-3iu6zt2tusazxrxi.mdb.yandexcloud.net:9870 > ~/.hdfscli.cfg""",
    )
    dump_hdfs_config = BashOperator(
        task_id='dump_hdfs_config',
        bash_command='cat ~/.hdfscli.cfg',
    )  
    query_db = BashOperator(
        task_id='query_db',
        bash_command='/opt/mssql-tools18/bin/sqlcmd -S 192.168.10.39 -d MIP_UtilizeOutbound_Main_Dev_Current -U userdb -P qwerty1 -C -Q "SELECT ID,CODE FROM dbo.Country" -W -w 1024  -I',
    )
    
    create_hdfs_config >> dump_hdfs_config >> query_db
   
  
