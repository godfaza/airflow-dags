import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="jupiter_dispatcher",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=["jupiter", "dev"],
) as dag:
   
    trigger_dag_qc = TriggerDagRunOperator(
        task_id="trigger_raw_qc",
        trigger_dag_id="jupiter_raw_qc",  
        conf={"parent_run_id":"{{run_id}}"},
        wait_for_completion = True,
    )
    
    trigger_dag_qc
