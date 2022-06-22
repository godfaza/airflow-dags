import datetime
import pendulum

from airflow import DAG
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

AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'

@task
def get_parameters(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds']
    run_id = urllib.parse.quote_plus(kwargs['run_id'])
    
    raw_path = Variable.get("RawPath")
    white_list = Variable.get("WhiteList")
    
    parameters = {"RawPath": raw_path,
                  "WhiteList": white_list,
                  "MaintenancePath":"{}{}{}_{}_".format(raw_path,"/#MAINTENANCE/",ds,run_id)
                  }
    print(parameters)
    return parameters


def _extract_db_schema(**context):
    parameters = context['ti'].xcom_pull(task_ids="get_parameters")
    dst_path = "{}/{}".format(parameters['RawPath'],
                              "/#MAINTENANCE/PARAMETERS.csv")
    query = mssql_scripts.generate_db_schema_query(
        white_list=parameters['WhiteList'])
    return query
#     print(query)

#     odbc_hook = OdbcHook()
#     hdfs_hook = WebHDFSHook()
#     conn = hdfs_hook.get_conn()

#     df =  odbc_hook.get_pandas_df(query)
#     df.to_csv('/tmp/PARAMETERS.csv', index=False)

#     conn.upload(dst_path,'/tmp/PARAMETERS.csv')


def _get_bcp_connections_string():
    conn = BaseHook.get_connection('jupiter_dev_mssql')
    print(
        f"Password: {conn.password}, Login: {conn.login}, URI: {conn.get_uri()}, Host: {conn.host}, Schema: {conn.schema}")
    return '-S {} -d {} -U {} -P {}'.format(conn.host, conn.schema, conn.login, conn.password)


def _generate_upload_scripts(**context):
    parameters = context['ti'].xcom_pull(task_ids="get_parameters")
    src_path = context['ti'].xcom_pull(task_ids="get_parameters",key="MaintenancePath")+"EXTRACT_ENTITIES_AUTO.csv"
    print(src_path)
    hdfs_hook = WebHDFSHook()
    conn = hdfs_hook.get_conn()
    conn.download(src_path, '/tmp/PARAMETERS.csv')
    
    out_query = mssql_scripts.generate_table_select_query('2022-06-20','2022-06-20','/tmp/PARAMETERS.csv')
    print(out_query)
    return  out_query

def _iterate_upload_scripts(**context):
    parameters = context['ti'].xcom_pull(task_ids="get_parameters")
    df = pd.read_csv(StringIO(context['ti'].xcom_pull(task_ids="generate_upload_scripts")),keep_default_na=False)
    df = df.reset_index()
    bcp_parameters = context['ti'].xcom_pull(task_ids="get_bcp_parameters")

    for index, row in df.iterrows():
     print(row['EntityName'], row['Extraction'])
     dst_path = "{}/{}.csv".format(parameters["RawPath"],row['EntityName'])
     command = 'cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query.sh && ~/exec_query.sh "{query}" {dst_path} "{bcp_parameters}" '.format(query=row['Extraction'],dst_path=dst_path,bcp_parameters=bcp_parameters)
     print(command)
     ret = subprocess.run(command, capture_output=True, shell=True)
     print(ret)
#      print('#######################-----------------------#######################')


with DAG(
    dag_id='jupiter_raw_data_upload',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jupiter", "dev"],
    render_template_as_native_obj=True,
) as dag:
    extract_db_schema = PythonOperator(
        task_id='extract_db_schema',
        python_callable=_extract_db_schema,
        provide_context=True,
    )

    get_bcp_parameters = PythonOperator(
        task_id='get_bcp_parameters',
        python_callable=_get_bcp_connections_string,
    )

    save_db_schema = BashOperator(
        task_id='save_db_schema',
        #           bash_command='echo "{{ ti.xcom_pull(task_ids="test-task") }}"',
        bash_command='cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query.sh && ~/exec_query.sh "{{ti.xcom_pull(task_ids="extract_db_schema")}}" {{ti.xcom_pull(task_ids="get_parameters",key="MaintenancePath")}}EXTRACT_ENTITIES_AUTO.csv "{{ti.xcom_pull(task_ids="get_bcp_parameters")}}" "Schema,TableName,FieldName,Position,FieldType,Size,IsNull,UpdateDate,Scale"',
    )

    generate_upload_scripts = PythonOperator(
        task_id='generate_upload_scripts',
        python_callable=_generate_upload_scripts,
        provide_context=True,
    )
    iterate_upload_scripts = PythonOperator(
        task_id='iterate_upload_scripts',
        python_callable=_iterate_upload_scripts,
        provide_context=True,
    )

    get_parameters >> get_bcp_parameters >> extract_db_schema >> save_db_schema >> generate_upload_scripts >> iterate_upload_scripts
