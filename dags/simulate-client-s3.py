from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import requests

from example.tasks import _load_esg_data_to_s3, _send_alert_discord

SYMBOL = "AAPL"

default_args = {
        'owner': 'airflow',
        'start_date': datetime.strptime("2022-04-11 20:00:00", "%Y-%m-%d %H:%M:%S"),
        'end_date': None,
        'retries': 0,
        'retry_delay': timedelta(minutes=0),
        'depends_on_past': False,
        'on_failure_callback': _send_alert_discord
    }        

@dag(
    start_date=datetime(2024,1,1),
    schedule='@daily',
    catchup=False,
    default_args = default_args,
    tags=["esg_demo"]
)

def simulate_client_s3():
    
    load_esg_data_to_s3 = PythonOperator(
        task_id = "load_esg_data_to_s3",
        python_callable=_load_esg_data_to_s3,
        op_kwargs = { 'path': '/opt/airflow/include/data/external/'}
    ) 

    load_esg_data_to_s3

simulate_client_s3()