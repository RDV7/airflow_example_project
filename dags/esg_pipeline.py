from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator 

from datetime import datetime, timedelta
import requests

from include.helpers.example.tasks import _apply_transform_module, _send_alert_discord, BUCKET_NAME

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
    tags=["esg_pipeline"]
)

def esg_pipeline():
    apply_transform_module = PythonOperator(
        task_id = 'apply_transform_module',
        python_callable = _apply_transform_module,
        # op_kwargs = { 'path': '{{ task_instance.xcom_pull(task_ids = "_load_esg_data_to_s3") }}'}
    )


#     transform_data = DockerOperator(
#         task_id = "transform_data",
#         image = "airflow/stock-app",
#         container_name = "transform_data",
#         api_version = 'auto',
#         auto_remove = True,
#         docker_url = 'tcp://docker-proxy:2375',
#         network_mode = 'container:spark-master',
#         tty = True,
#         xcom_all = False,
#         mount_tmp_dir = False,
#         environment = {
#             'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids = "load_stock_prices_to_s3") }}'
#         }
#     )

    # get_formatted_prices = PythonOperator(
    #     task_id = 'get_formatted_prices',
    #     python_callable = _get_formatted_prices,
    #     op_kwargs = { 'path': '{{ task_instance.xcom_pull(task_ids = "_load_esg_data_to_s3") }}'}
    # )

    # transform_data >> get_formatted_prices
    apply_transform_module

esg_pipeline()