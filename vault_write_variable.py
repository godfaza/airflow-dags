import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.yandex.operators.yandexcloud_dataproc import  DataprocCreatePysparkJobOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
import uuid
from io import StringIO
import urllib.parse
import subprocess

import cloud_scripts.mssql_scripts as mssql_scripts
import json
import pandas as pd
import glob
import os


MSSQL_CONNECTION_NAME = 'odbc_default'
HDFS_CONNECTION_NAME = 'webhdfs_default'
AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'

MONITORING_DETAIL_DIR_PREFIX = 'MONITORING_DETAIL.CSV'
EXTRACT_ENTITIES_AUTO_FILE = 'EXTRACT_ENTITIES_AUTO.csv'
MONITORING_FILE = 'MONITORING.csv'

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
    system_name = Variable.get("SystemName")
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters = '-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login, db_conn.password)
    
    parameters = {"RawPath": raw_path,
                  "WhiteList": white_list,
                  "MaintenancePath":"{}{}{}_{}_".format(raw_path,"/#MAINTENANCE/",ds,run_id),
                  "BcpParameters": bcp_parameters,
                  "UploadPath": upload_path,
                  "RunId":run_id,
                  "SystemName":system_name,
                  }
    print(parameters)
    return parameters

@task
def generate_schema_query(parameters: dict):
    query = mssql_scripts.generate_db_schema_query(
        white_list=parameters['WhiteList'])
    
    return query


with DAG(
    dag_id='vault_write_variable',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jupiter", "dev"],
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
#     Generate schema extraction query
    schema_query = generate_schema_query(parameters)
    
    

