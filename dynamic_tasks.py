import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='dynamic_tasks',
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

  for i, table_name in enumerate(tables):
    a.append(DummyOperator(
        task_id='Component_'+str(table_name),
        dag=dag))
    if i not in [0]: 
        a[i-1] >> a[i]
   
  
