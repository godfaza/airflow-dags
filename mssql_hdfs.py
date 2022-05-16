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

    download_schema = BashOperator(
        task_id='download_schema',
        bash_command="cp -r /opt/airflow/logs/src/. ~/ && chmod +x ~/download_schema.sh && ~/download_schema.sh ",
            )
    download_table = BashOperator(
        task_id='download_table',
        bash_command="cp -r /opt/airflow/logs/src/. ~/ && chmod +x ~/download_table.sh && ~/download_table.sh {{params.table_name}} ",
        params = {'table_name':'YA_DATAMART4'},
        dag=dag)
    query_db = BashOperator(
        task_id='query_db',
        bash_command='/opt/mssql-tools18/bin/sqlcmd -S 192.168.10.39 -d MIP_UtilizeOutbound_Main_Dev_Current -U userdb -P qwerty1 -C -Q "SELECT ID,CODE FROM dbo.Country" -W -w 1024  -I',
    )
    
    download_schema>> download_table >> query_db
   
  
