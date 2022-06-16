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
    odbc_hook = OdbcHook( default_conn_name = "jupiter_odbc_default") 
    cnxn = odbc_hook.get_conn()

    cursor = cnxn.cursor()
    cursor.execute("SELECT @@SERVERNAME, @@VERSION;")
    row = cursor.fetchone()
    while row:
        print("Server Name:" + row[0])
        print("Server Version:" + row[1])
        row = cursor.fetchone()

PythonOperator(
    task_id="sample_select",
    python_callable=sample_select,
    dag=dag,
)
