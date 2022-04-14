from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

query = """SELECT 
               toDate(time) as event_date, 
               country, 
               source,
               count() as likes
            FROM 
                simulator_20220320.feed_actions 
            where 
                toDate(time) = today()-1 
                and action = 'like'
            group by
                event_date,
                country,
                source
            format TSVWithNames"""

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'o-fedorova-3',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '30 14 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_ofedorova():
    
    @task()
    def extract():
        query = """SELECT 
               toDate(time) as event_date, 
               country, 
               source,
               count() as likes
            FROM 
                simulator_20220320.feed_actions 
            where 
                toDate(time) = today()-1 
                and action = 'like'
            group by
                event_date,
                country,
                source
            format TSVWithNames"""
        df_cube = ch_get_df(query=query)
        return df_cube
    
    df_cube = extract()
    
dag_etl_ofedorova() = dag_etl_ofedorova()
