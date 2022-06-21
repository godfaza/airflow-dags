import time

from airflow.decorators import task, dag
from datetime import datetime
from pathlib import Path


logs_file = Path("/data/dummy_logs/random_logs.log")


def get_process_id(line: str) -> str:
   return line.split(" ")[-1]


@dag(start_date=datetime(2022, 5, 16),
    schedule_interval="@daily",
    dag_id="repair_components_dummy",
    catchup=False)
def my_dag():
   @task
   def get_failed_processes(log_file: Path) -> list[str]:
       failed_processes = []

       with open(log_file, 'r') as f:
           for line in f:
               if line.startswith("ERROR"):
                   failed_processes.append(get_process_id(line))

       return failed_processes

   @task
   def repair_process(process_id: str):
       time.sleep(0.5)  # Simulate computation time
       print(f"process {process_id} repaired")

   repair_process.expand(process_id=get_failed_processes(logs_file))

repair_logs_dummy = my_dag()
