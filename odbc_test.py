# this is not production code. just useful for testing connectivity.
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="odbc_example",
    default_args={},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
    catchup=False,
)

def sample_select():
    odbc_hook = OdbcHook() 
    rec = odbc_hook.get_records("SELECT * from Country;")
    print(rec)
#     cnxn = odbc_hook.get_conn()

#     cursor = cnxn.cursor()
#     cursor.execute("SELECT * from Country;")
#     row = cursor.fetchone()
#     while row:
#         print(row[2])
#         row = cursor.fetchone()

PythonOperator(
    task_id="sample_select",
    python_callable=sample_select,
    dag=dag,
)
