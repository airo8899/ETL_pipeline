# coding=utf-8

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
from io import StringIO
#from airflow.operators.python import get_current_context
import requests


# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 2, 15),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

# Ботик телеграма, в который будут приходить статусы выполнения
# def send_message(context):
#     if BOT_TOKEN:
#         bot = telegram.Bot(token=BOT_TOKEN)
#         dag_id = context['dag'].dag_id
#         message = f'Huge success. Dag {dag_id} completed'
#         bot.send_message(chat_id=BOT_CHAT, text=message)
#     else:
#         pass


@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def dag_simulator():

    @task()
    def extract():
        query = """SELECT 
                       toDate(time) as event_date, 
                       country, 
                       source,
                       count() as likes
                    FROM 
                        simulator.feed_actions 
                    where 
                        toDate(time) = '2022-01-26' 
                        and action = 'like'
                    group by
                        event_date,
                        country,
                        source
                    format TSVWithNames"""
        df_cube = ch_get_df(query)
        return df_cube

    @task()
    def transform_countries(df_cube):
        countries = df_cube[['event_date', 'country', 'likes']]\
                            .groupby(['event_date', 'country'])\
                            .sum()\
                            .reset_index()
        return countries

    @task()
    def transform_sources(df_cube):
        sources = df_cube[['event_date', 'source', 'likes']]\
                          .groupby(['event_date', 'source'])\
                          .sum()\
                          .reset_index()
        return sources

    @task()
    def load(countries, sources, ds):
        print(f'Likes by country for date {ds}')
        print(countries.to_csv(index=False, header=False))
        print(f'Likes by source for date {ds}')
        print(sources.to_csv(index=False, header=False))

    df_cube = extract()
    countries = transform_countries(df_cube)
    sources = transform_sources(df_cube)
    load(countries, sources)

dag_test = dag_simulator()