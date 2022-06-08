
"""
Example usage of the TriggerDagRunOperator. This example holds 2 DAGs:
1. 1st DAG (example_trigger_controller_dag) holds a TriggerDagRunOperator, which will trigger the 2nd DAG
2. 2nd DAG (example_trigger_target_dag) which will be triggered by the TriggerDagRunOperator in the 1st DAG
"""
import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="upload_controller",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=['example'],
) as dag:
    download_schema = BashOperator(
        task_id='download_schema',
        bash_command="cp -r /tmp/data/src/. ~/ && chmod +x ~/download_schema.sh && ~/download_schema.sh ",
            )
    
    trigger = TriggerDagRunOperator(
        task_id="trigger_raw_data_upload",
        trigger_dag_id="raw_data_upload",  
        conf={"message": "Starting child dag"},
        wait_for_completion = True,
    )
    
    download_schema >> trigger
