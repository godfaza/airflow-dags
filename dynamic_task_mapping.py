import json
import pendulum
from airflow import DAG
from airflow.decorators import dag, task

@task
def make_list():
    # This can also be from an API call, checking a database, -- almost anything you like, as long as the
    # resulting list/dictionary can be stored in the current XCom backend.
    return [1, 2, {"a": "b"}, "str"]


@task
def consumer(arg):
    print(list(arg))


with DAG(dag_id="dynamic-map", start_date=datetime(2022, 4, 2)) as dag:
    consumer.expand(arg=make_list())
