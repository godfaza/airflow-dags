import json
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task

@task
def make_list():
    # This can also be from an API call, checking a database, -- almost anything you like, as long as the
    # resulting list/dictionary can be stored in the current XCom backend.
    return [1, 2, {"a": "b"}, "str"]


@task
def consumer(arg):
    print(list(arg))


with DAG(dag_id="dynamic-map", 
         start_date=datetime(2022, 4, 2),
         interval = None
        ) as dag:
      echo_op=BashOperator.partial(task_id="bash", do_xcom_push=False).expand(
       bash_command=["echo 1", "echo 2"]
    )
#     consumer.expand(arg=make_list())
