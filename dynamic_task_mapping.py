import json
import pendulum
from airflow.decorators import dag, task

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def tutorial_taskflow_api_etl():
    @task
    def add_one(x):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        
        print(f"Total was {total}")

    added_values = add_one.expand(x=[1, 2, 3])
    sum_it(added_values)
    
tutorial_etl_dag = tutorial_taskflow_api_etl()
