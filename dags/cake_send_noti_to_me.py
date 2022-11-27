
import requests

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from aircake.logger import loggerFactory

logger = loggerFactory(__name__)

import datetime

API_TOKEN  = "5140213392:AAGyEUf2-HtEAbjJ_gFYlj8b3LbQJ0_BiZY"; # please not share
CHAT_ID    = "-1001669496209";
TEXT       = f"Good day {datetime.datetime.now()}"

url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage?chat_id={CHAT_ID}&parse_mode=HTML&text={TEXT}";

def send_tele():
   requests.post(url)
   return

with DAG("cake_send_noti_to_me",
         schedule_interval="@daily",
         start_date=days_ago(1)) as dag:
    
    send_noti = PythonOperator(task_id='send_noti', python_callable=send_tele)
    send_noti