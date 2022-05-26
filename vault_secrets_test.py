from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def print_var():
    my_var = Variable.get("airflow")
    print(f'My variable is: {my_var}')

with DAG('vault_secrets_test', start_date=datetime(2022, 1, 1), schedule_interval=None) as dag:
  ping = BashOperator(
        task_id='ping',
        bash_command="echo $AIRFLOW__SECRETS__BACKEND_KWARGS",
            )
  test_task = PythonOperator(
      task_id='test-task',
      python_callable=print_var,
    )
  
  ping >> test_task
