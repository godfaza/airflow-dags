import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='object_storage_mount',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    check_mount = BashOperator(
        task_id='check_mount',
        bash_command="ls -la /source-code",
        executor_config={"KubernetesExecutor": {
                                  "image": "airflow:runner2",
                                  "volume_mounts": [
                                      {
                                          "name": "airlow-source-code",
                                          "mountPath": "/tmp/source-code"
                                      }],
                                  }
                             },
    )

    check_mount
   
  
