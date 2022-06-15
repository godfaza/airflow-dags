import os
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


with DAG(
    'mssql_test',
    schedule_interval=None,
    start_date=datetime(2021, 10, 1),
    tags=['mssql_test'],
    catchup=False,
) as dag:
    query1 = MsSqlOperator(
        task_id="query1",
        mssql_conn_id='jupiter_dev_mssql',
        sql=r"""SELECT * FROM Country;""",
    )
    
    query1
