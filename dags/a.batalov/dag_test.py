from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
from io import StringIO
#from airflow.operators.python import get_current_context
import requests


def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text))
    return result


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 2, 15),
}

schedule_interval = '0 23 * * *'

# def send_message(context):
#     if BOT_TOKEN:
#         bot = telegram.Bot(token=BOT_TOKEN)
#         dag_id = context['dag'].dag_id
#         message = f'Huge success. Dag {dag_id} completed'
#         bot.send_message(chat_id=BOT_CHAT, text=message)
#     else:
#         pass


@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def dag_test():
    @task()
    def task_test():
        query = """SELECT * FROM simulator.feed_actions where toDate(time) = '2022-01-26' limit 10"""
        df = ch_get_df(query)
        print(df.head())

    task_test()

dag_test = dag_test()