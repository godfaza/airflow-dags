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
#     source_name = Variable.get("SourceName")
#     raw_path = Variable.get("RawPath")
#     process_path = Variable.get("ProcessPath")
#     output_path = Variable.get("OutputPath")
#     upload_date = Variable.get("UploadDate")
    return {}
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


   
  
