import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.yandex.operators.yandexcloud_dataproc import  DataprocCreatePysparkJobOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
import uuid
from io import StringIO
import urllib.parse
import subprocess

import cloud_scripts.mssql_scripts as mssql_scripts
import json
import pandas as pd


MSSQL_CONNECTION_NAME = 'odbc_default'
HDFS_CONNECTION_NAME = 'webhdfs_default'
AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'

MONITORING_DETAIL_DIR_PREFIX='MONITORING_DETAIL.CSV'
STATUS_FAILURE='FAILURE'
STATUS_COMPLETE='COMPLETE'
STATUS_PROCESS='PROCESS'

def separator_convert_hex_to_string(sep):
    sep_map = {'0x01':'\x01'}
    return sep_map.get(sep, sep)

@task(multiple_outputs=True)
def get_parameters(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds']
    execution_date = kwargs['execution_date'].strftime("%Y/%m/%d")
    run_id = urllib.parse.quote_plus(kwargs['run_id'])
    
    raw_path = Variable.get("RawPath")
    white_list = Variable.get("WhiteList")
    upload_path = f'{raw_path}/{execution_date}/'
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters = '-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login, db_conn.password)
    
    parameters = {"RawPath": raw_path,
                  "WhiteList": white_list,
                  "MaintenancePath":"{}{}{}_{}_".format(raw_path,"/#MAINTENANCE/",ds,run_id),
                  "BcpParameters": bcp_parameters,
                  "UploadPath": upload_path,
                  "RunId":run_id,
                  }
    print(parameters)
    return parameters

@task
def generate_schema_query(parameters: dict):
    query = mssql_scripts.generate_db_schema_query(
        white_list=parameters['WhiteList'])
    
    return query

@task
def copy_data_db_to_hdfs(query,dst_dir,dst_file):
    
    dst_path = f"{dst_dir}{dst_file}"
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()

    df =  odbc_hook.get_pandas_df(query)
    df.to_csv(f'/tmp/{dst_file}', index=False)
    conn.upload(dst_path,f'/tmp/{dst_file}')
    
    return True

@task    
def get_entities(prev_task,src_dir,src_file,upload_path,bcp_parameters):
    src_path = f"{src_dir}{src_file}"
    tmp_path = f"/tmp/{src_file}"
    print(src_path)
    
    hdfs_hook = WebHDFSHook()
    conn = hdfs_hook.get_conn()
    conn.download(src_path, tmp_path)
    
    entities = mssql_scripts.generate_table_select_query('2022-06-20','2022-06-20',tmp_path)
  
    return entities
    

@task    
def generate_upload_scripts(prev_task,src_dir,src_file,upload_path,bcp_parameters):
    src_path = f"{src_dir}{src_file}"
    tmp_path = f"/tmp/{src_file}"
    print(src_path)
    
    hdfs_hook = WebHDFSHook()
    conn = hdfs_hook.get_conn()
    conn.download(src_path, tmp_path)
    
    queries = mssql_scripts.generate_table_select_query('2022-06-20','2022-06-20',tmp_path)
    
    scripts_list = ['cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query.sh && ~/exec_query.sh "{}" {}{}/{}/{}/{}.csv "{}" {} "{}" '.format("select proc from table1;",upload_path,x["Schema"],x["EntityName"],x["Method"],x["EntityName"],bcp_parameters,BCP_SEPARATOR,x["Columns"].replace(",",separator_convert_hex_to_string(BCP_SEPARATOR))) for x in queries]
    print(scripts_list)
    return  scripts_list

@task
def start_monitoring(dst_dir,upload_path,input):
    schema = input["Schema"]
    entity_name = input["EntityName"]
    method = input["Method"]
    monitoring_file_path=f'{dst_dir}{MONITORING_DETAIL_DIR_PREFIX}/{schema}_{entity_name}.csv'
    print(monitoring_file_path)
    temp_file_path =f'/tmp/{shema}_{entity_name}.csv'
    df = pd.DataFrame([{'PipelineRunId':urllib.parse.quote_plus(run_id),
                        'Schema':schema,
                        'EntityName':entity_name,
                        'TargetPath':f'{upload_path}{schema}/{entity_name}/{method}/{entity_name}.csv',
                        'TargetFormat':'CSV',
                        'StartDate':pendulum.now(),
                        'Duration':0,
                        'Status':STATUS_PROCESS,
                        'ErrorDescription':None
                       }])
    df.to_csv(monitoring_file_path, index=False)
    
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.upload(monitoring_file_path,temp_file_path)
    
    return input
#     l = list(input)
#     print(l)

@task
def end_monitoring(dst_dir,input):
    print(input)
    return input
#     l = list(input)
#     print(l)

    

with DAG(
    dag_id='jupiter_raw_data_upload',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jupiter", "dev"],
    render_template_as_native_obj=True,
) as dag:
    
    parameters = get_parameters()
    schema_query = generate_schema_query(parameters)
    extract_schema = copy_data_db_to_hdfs(schema_query,parameters["MaintenancePath"],"EXTRACT_ENTITIES_AUTO.csv")
    start_mon = start_monitoring.partial(dst_dir=parameters["MaintenancePath"],upload_path=parameters["UploadPath"]).expand(input = get_entities(extract_schema,parameters["MaintenancePath"],"EXTRACT_ENTITIES_AUTO.csv",parameters["UploadPath"],parameters["BcpParameters"]))
    end_mon = end_monitoring.partial(dst_dir=parameters["MaintenancePath"]).expand(input = start_mon)
    #     upload_tables=BashOperator.partial(task_id="upload_tables", do_xcom_push=True).expand(
#        bash_command=generate_upload_scripts(extract_schema,parameters["MaintenancePath"],"EXTRACT_ENTITIES_AUTO.csv",parameters["UploadPath"],parameters["BcpParameters"])  ,
#     )
    

#     monitoring_results = save_monitoring_result(XComArg(upload_tables))
    
#     upload_tables >> monitoring_results
