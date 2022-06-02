from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook

def get_secrets(**kwargs):
     conn = BaseHook.get_connection(kwargs['my_conn_id'])
     print(f"Password: {conn.password}, Login: {conn.login}, URI: {conn.get_uri()}, Host: {conn.host}, Schema: {conn.schema}")

with DAG('vault_connections_test', start_date=datetime(2020, 1, 1), schedule_interval=None) as dag:

     test_task = PythonOperator(
         task_id='test-task',
         python_callable=get_secrets,
         op_kwargs={'my_conn_id': 'jupiter_dev_mssql'},
     )
     download_schema = BashOperator(
        task_id='exec_query',
        bash_command='cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query.sh && ~/exec_query.sh "select 1" /user/smartadmin/schema/query_out.csv {{params.host}} {{params.schema}} {{params.login}} {{params.password}} ',
        params = BaseHook.get_connection('jupiter_dev_mssql'),  
            )
