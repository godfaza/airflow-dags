import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
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

AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'

with DAG(
    dag_id='etl_test1',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

 
  def read_tables_list():
   import sys
   import hdfs
  
   cl=hdfs.client.Client(url="http://rc1b-dataproc-m-3iu6zt2tusazxrxi.mdb.yandexcloud.net:9870")
   src = "/user/smartadmin/schema/schema.csv"
   dst = "/tmp/schema.csv" 
   with cl.read(src, encoding='utf-8') as reader:
    file = reader.read()
    lines = file.splitlines()
    print(lines)
    return lines
   
  tables = read_tables_list()

  t0 = DummyOperator(task_id='start')

  with TaskGroup(group_id='tables_to_datasets') as tg1:
    for i, entity_name in enumerate(tables):
      download_table = BashOperator(
        task_id='download_table_{}'.format(entity_name),
        bash_command="cp -r /tmp/data/src/. ~/ && chmod +x ~/download_table.sh && ~/download_table.sh {{params.table_name}} ",
        params = {'table_name':entity_name},
        dag=dag)

  with TaskGroup(group_id='datasets_to_tables') as tg2:
    for i, entity_name in enumerate(tables):    
      upload_dataset_to_db = BashOperator(
        task_id='upload_dataset_{}'.format(entity_name),
        bash_command="cp -r /tmp/data/src/. ~/ && chmod +x ~/upload_file.sh && ~/upload_file.sh {{params.file_name}} {{params.table_name}} ",
        params = {'file_name':entity_name,'table_name':'YA_DATAMART_FDM{}'.format(i+1)},
        dag=dag)
    
  transform = DataprocCreatePysparkJobOperator(
        task_id='transform',
        cluster_id='c9qc9m3jccl8v7vigq10',
        main_python_file_uri='s3a://jupiter-app-test-storage/src/EMPTY_JOB.py',
        file_uris=[
            's3a://data-proc-public/jobs/sources/data/config.json',
        ],
        args=[
            's3a://data-proc-public/jobs/sources/data/cities500.txt.bz2',
            f's3a://{S3_BUCKET_NAME_FOR_JOB_LOGS}/dataproc/job/results/${{JOB_ID}}',
        ],
        properties={
            'spark.submit.deployMode': 'cluster',
        },
        packages=['org.slf4j:slf4j-simple:1.7.30'],
        repositories=['https://repo1.maven.org/maven2'],
        exclude_packages=['com.amazonaws:amazon-kinesis-client'],
    )
    
  t0 >> tg1 >> transform >> tg2


   
  
