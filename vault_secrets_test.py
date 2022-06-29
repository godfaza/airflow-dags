from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.configuration import ensure_secrets_loaded
from airflow.providers.hashicorp.secrets.vault import VaultBackend
from airflow.providers.hashicorp.hooks.vault import VaultHook

def print_var():
    my_var = Variable.get("var999")
    print(f'My variable is: {my_var}')
    
    vault_hook = VaultHook()
    conn = vault_hook.get_conn()
    conn.secrets.kv.v1.create_or_update_secret(path="variables/var999",secret={"value":"AIRFLOW_UPD1"})
#     for secrets_backend in ensure_secrets_loaded():
#       if isinstance(secrets_backend, VaultBackend):
#         print(secrets_backend.vault_client.mount_point)
#         secrets_backend.vault_client.client.secrets.kv.v1.create_or_update_secret(path="variables/var999",secret={"value":"AIRFLOW_UPD"})
    

with DAG('vault_secrets_test', start_date=datetime(2022, 1, 1), schedule_interval=None) as dag:
  ping = BashOperator(
        task_id='ping',
        bash_command="echo $AIRFLOW__SECRETS__BACKEND_KWARGS",
            )
  test_task = PythonOperator(
      task_id='test-task',
      python_callable=print_var,
    )

