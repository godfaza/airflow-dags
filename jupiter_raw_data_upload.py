import datetime
import pendulum

from airflow import DAG
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import uuid
from io import StringIO
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

def get_db_schema(**context):
    parameters = context['ti'].xcom_pull(task_ids="get_parameters_from_kv")
    dst_path = "{}/{}".format(parameters['RawPath'],"/#MAINTENANCE/PARAMETERS.csv")
    query =  mssql_scripts.generate_db_schema_query(white_list=parameters['WhiteList'])
    return query
#     print(query)
    
#     odbc_hook = OdbcHook()
#     hdfs_hook = WebHDFSHook()
#     conn = hdfs_hook.get_conn()
    
#     df =  odbc_hook.get_pandas_df(query)
#     df.to_csv('/tmp/PARAMETERS.csv', index=False)
    
#     conn.upload(dst_path,'/tmp/PARAMETERS.csv')

def get_bcp_connections_string():
     conn = BaseHook.get_connection('jupiter_dev_mssql')
     print(f"Password: {conn.password}, Login: {conn.login}, URI: {conn.get_uri()}, Host: {conn.host}, Schema: {conn.schema}")
     return '-S {} -d {} -U {} -P {}'.format(conn.host,conn.schema,conn.login,conn.password)

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
      provide_context=True,
    )
    
  get_bcp_parameters = PythonOperator(
      task_id='extract_db_schema',
      python_callable=get_bcp_connections_string,
    )  
    
  save_db_schema = BashOperator(
        task_id='exec_query',
#           bash_command='echo "{{ ti.xcom_pull(task_ids="test-task") }}"',
        bash_command='cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query.sh && ~/exec_query.sh "select * from Country;" /user/smartadmin/schema/query_out.csv "{{ti.xcom_pull(task_ids="get_bcp_parameters")}}"',
            )  
    
  get_parameters_from_kv >>  get_bcp_parameters >>  extract_db_schema >> save_db_schema


   
  
