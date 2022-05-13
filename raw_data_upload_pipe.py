from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def create_dag(dag_id,
               schedule,
               dag_number,
               default_args):

    def hello_world_py(*args):
        print('Hello World')
        print('This is DAG: {}'.format(str(dag_number)))

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        t1 = PythonOperator(
            task_id='hello_world',
            python_callable=hello_world_py)

    return dag

def read_tables_list():
  import sys
  import hdfs
  
  cl=hdfs.client.Client(url="http://rc1b-dataproc-m-3iu6zt2tusazxrxi.mdb.yandexcloud.net:9870")
  src = "/user/smartadmin/schema/schema.csv"
  dst = "~/schema.csv" 
  print("from={} to={}".format(src,dst))
  cl.download(src,dst)
  
   with open(dst) as file:
    lines = file.readlines()
    lines = [line.rstrip() for line in lines]
  
  print(lines)

read_tables_list()
# build a dag for each number in range(10)
for n in range(1, 4):
    dag_id = 'raw_data_upload_pipe_{}'.format(str(n))

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2021, 1, 1)
                    }

    schedule = '@daily'
    dag_number = n

    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  dag_number,
                                  default_args)
