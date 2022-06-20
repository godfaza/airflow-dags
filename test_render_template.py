import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook

def create_config():
    return {
        'key': 'value'
    }


def read_config(config):
    print(type(config))
    print(config)


with DAG(
    "test_render_template",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    render_template_as_native_obj=True
) as dag:
    create_config = PythonOperator(
        task_id="create_config",
        python_callable=create_config
    )
    read_config = BashOperator(
        task_id="read_config",
        bash_command='echo "{{ ti.xcom_pull(task_ids='create_config')['key'] }}" ',
      )
    create_config >> read_config
