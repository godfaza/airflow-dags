import json
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

# @task
# def make_list():
#     # This can also be from an API call, checking a database, -- almost anything you like, as long as the
#     # resulting list/dictionary can be stored in the current XCom backend.
#     return ["echo DS1","echo DS2","echo DS3"]


@task
def consumer(arg):
    print(list(arg))
    
def _make_list():
    return ["echo DS1","echo DS2","echo DS3"]    


with DAG(dag_id="dynamic-map", 
         start_date=datetime(2022, 4, 2),
         schedule_interval=None,
        ) as dag:
    
      make_list = PythonOperator(
        task_id='make_list',
        python_callable=_make_list,
        provide_context=True,
    )
      echo_op=BashOperator.partial(task_id="bash", do_xcom_push=False).expand(
       bash_command=self.make_list(),
    )
#     consumer.expand(arg=make_list())
