from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook

def get_secrets(**kwargs):
     conn = BaseHook.get_connection(kwargs['default_email])
     print(conn)
     print(f"Password: {conn.password}, Login: {conn.login}, URI: {conn.get_uri()}, Host: {conn.host}")

with DAG('vault_connections_test', start_date=datetime(2020, 1, 1), schedule_interval=None) as dag:

     test_task = PythonOperator(
         task_id='test-task',
         python_callable=get_secrets,
     )
