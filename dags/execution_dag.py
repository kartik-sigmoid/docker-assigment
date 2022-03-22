from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from script import create_table_and_populate_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 22),
    "retries": 0,
}


with DAG("execution", default_args=default_args, schedule_interval="0 6 * * *", catchup = False) as dag:

    t1 = DummyOperator(task_id = "verify")

    t2 = PythonOperator(task_id = "create_and_fill_table", python_callable = create_table_and_populate_data)
    
    t1 >> t2
