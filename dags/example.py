from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator 

# from astro import sql as aql
# from astro.files import File 
# from astro.sql.table import Table, Metadata

from datetime import datetime, timedelta
import requests

from example.tasks import _get_stock_prices, _load_stock_prices_to_s3, _get_formatted_prices, _send_alert_discord, BUCKET_NAME

SYMBOL = "AAPL"

# 'email': [],
# 'email_on_failure': False,
# 'email_on_retry': False,
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
    tags=["stock_market"]
)

def stock_market():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('yahoo-stock-api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id = "get_stock_prices",
        python_callable = _get_stock_prices,
        op_kwargs = { 'url': '{{ task_instance.xcom_pull(task_ids = "is_api_available") }}', 'symbol': SYMBOL}
    )

    load_stock_prices_to_s3 = PythonOperator(
        task_id = "load_stock_prices_to_s3",
        python_callable=_load_stock_prices_to_s3,
        op_kwargs = { 'stock': '{{ task_instance.xcom_pull(task_ids = "get_stock_prices") }}'}
    ) 

    format_prices = DockerOperator(
        task_id = "format_prices",
        image = "airflow/stock-app",
        container_name = "format_prices",
        api_version = 'auto',
        auto_remove = True,
        docker_url = 'tcp://docker-proxy:2375',
        network_mode = 'container:spark-master',
        tty = True,
        xcom_all = False,
        mount_tmp_dir = False,
        environment = {
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids = "load_stock_prices_to_s3") }}'
        }
    )

    get_formatted_prices = PythonOperator(
        task_id = 'get_formatted_prices',
        python_callable = _get_formatted_prices,
        op_kwargs = { 'path': '{{ task_instance.xcom_pull(task_ids = "load_stock_prices_to_s3") }}'}
    )

    # load_to_postgres = aql.load_file(
    #     task_id = 'load_to_postgres',
    #     input_file = File(path=f"s3://{BUCKET_NAME}/{{{{ task_instance.xcom_pull(task_ids = 'get_formatted_prices') }}}}", conn_id = 'minio'),
    #     output_table = Table(
    #         name = 'stock_market',
    #         conn_id = 'postgres',
    #         metadata = Metadata(
    #             schema = 'public'
    #         )
    #     ),
    #     load_options={
    #         "aws_access_key_id": BaseHook.get_connection('minio').login,
    #         "aws_secret_access_key": BaseHook.get_connection('minio').password,
    #         "endpoint_url": BaseHook.get_connection('minio').host,
    #     }
    # )

    is_api_available() >> get_stock_prices >> load_stock_prices_to_s3 >> format_prices >> get_formatted_prices 
    # >> load_to_postgres

     

stock_market()