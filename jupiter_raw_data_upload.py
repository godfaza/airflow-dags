import datetime
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import uuid
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreateHiveJobOperator,
    DataprocCreateMapReduceJobOperator,
    DataprocCreatePysparkJobOperator,
    DataprocCreateSparkJobOperator,
    DataprocDeleteClusterOperator,
)
import mssql_scripts

AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'

def get_parameters():
    parameters = Variable.get("JupiterParameters",deserialize_json=True)
    print(parameters)
    return parameters

def get_db_schema(**kwargs):
    query =  mssql_scripts.generate_db_schema_query(white_list=kwargs['white_list'])
    print(query)

with DAG(
    dag_id='jupiter_raw_data_upload',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jupiter","dev"],
) as dag:
  get_parameters_from_kv = PythonOperator(
      task_id='get_parameters_from_kv',
      python_callable=get_parameters,
    )

  extract_db_schema = PythonOperator(
      task_id='extract_db_schema',
      python_callable=get_db_schema,
      op_kwargs={{ti.xcom_pull(task_ids="get_parameters_from_kv")},
    )
  get_parameters_from_kv >>  extract_db_schema 


   
  
