from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task

import random

with DAG(dag_id='dynamic_task_mapping_example', start_date=days_ago(2), catchup=False) as dag:

    @task
    def make_list():
        return [i + 1 for i in range(random.randint(2, 4))]

    @task
    def consumer(value):
        print(repr(value))

    consumer.expand(value=make_list())
