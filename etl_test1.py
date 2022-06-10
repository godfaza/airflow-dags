import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

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
  a = []
  for i, entity_name in enumerate(tables):
    download_table = BashOperator(
        task_id='download_table_{}'.format(entity_name),
        bash_command="cp -r /tmp/data/src/. ~/ && chmod +x ~/download_table.sh && ~/download_table.sh {{params.table_name}} ",
        params = {'table_name':entity_name},
        dag=dag)
    
    upload_dataset_to_db = BashOperator(
        task_id='upload_dataset_{}'.format(entity_name),
        bash_command="cp -r /tmp/data/src/. ~/ && chmod +x ~/upload_file.sh && ~/upload_file.sh {{params.file_name}} {{params.table_name}} ",
        params = {'file_name':entity_name,'table_name':'YA_DATAMART_FDM{}'.format(i+1)},
        dag=dag)
    
    download_table >> upload_dataset_to_db


   
  
