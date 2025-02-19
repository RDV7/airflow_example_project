from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
import requests
import json
from example.minio_helper import get_minio_client
from io import BytesIO
import pandas as pd
import os  
import re

from airflow import DAG
from airflow.models import Variable, TaskInstance
# from airflow.utils.email import send_email

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
TI = TaskInstance

BUCKET_NAME = 'esg-test-bucket'

def _send_alert_discord(context):
    # Get Task Instances variables
    last_task: Optional[TaskInstance] = context.get('task_instance')
    task_name = last_task.task_id
    dag_name = last_task.dag_id
    log_link = last_task.log_url
    execution_date = str(context.get('execution_date'))

    # Extract reason for the exception
    try:
        error_message = str(context["exception"])
        error_message = error_message[:1000] + (error_message[1000:] and '...')
        str_start = re.escape("{'reason': ")
        str_end = re.escape('"}.')
        error_message = re.search('%s(.*)%s' % (str_start, str_end), error_message).group(1)
        error_message = "{'reason': " + error_message + '}'
    except:
        error_message = "Some error that cannot be extracted has occurred. Visit the logs!"

    # print(Variable.get("discord_webhook"))

    # Send Alert
    # x = Variable.get("discord_webhook")
    api = BaseHook.get_connection('discord').extra_dejson
    
    webhook = DiscordWebhookHook(http_conn_id=api['conn_id'], webhook_endpoint=api['webhook_endpoint'], message="Airflow Alert - Task has failed!")
    # embed = DiscordEmbed(title="Airflow Alert - Task has failed!", color='CC0000', url=log_link, timestamp=execution_date)
    # embed.add_embed_field(name="DAG", value=dag_name, inline=True)
    # embed.add_embed_field(name="PRIORITY", value="HIGH", inline=True)
    # embed.add_embed_field(name="TASK", value=task_name, inline=False)
    # embed.add_embed_field(name="ERROR", value=error_message)
    # webhook.add_embed(embed)
    response = webhook.execute()

    return response

def _apply_transform_module() -> str:
    client = get_minio_client() 
    
    file = 'EmissionsInput.json'

    objw = client.get_object(
            bucket_name=BUCKET_NAME,
            object_name=f'test_esg/{file}'
    )

    df = pd.read_json(objw)

    df = df.assign(Product=lambda x: (x['Emissions_CO2eq'] * x['Emissions_CO2eq'] + x['Emissions_CO2eq']))
    print(df.head)

    return f'{df.columns}'

def _load_esg_data_to_s3(path) -> str:
    
    client = get_minio_client() 
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    file_list = [ f for f in os.listdir(path) if f.endswith('.xlsx') ]

    s3_paths = []

    for file in file_list:

        print(f"writing file: {file}")        
        excel_df = pd.read_excel(path + file)
        df_to_json = excel_df.to_json(orient='records')  
        load = json.loads(df_to_json)
        data = json.dumps(load, ensure_ascii=False).encode("utf-8")

        objw = client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f'test_esg/{file.split('.')[0]}.json',
            data=BytesIO(data),
            length=len(data)
        )    

        s3_paths.append(objw.object_name)

    return ' '.join(s3_paths)

