# coding=utf-8

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
                simulator.feed_actions 
            where 
                toDate(time) = '2022-01-26' 
                and action = 'like'
            group by
                event_date,
                country,
                source
            format TSVWithNames"""

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_sim_example():

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
        df_cube = ch_get_df(query=query)
        return df_cube

    @task
    def transfrom_source(df_cube):
        df_cube_source = df_cube[['event_date', 'source', 'likes']]\
            .groupby(['event_date', 'source'])\
            .sum()\
            .reset_index()
        return df_cube_source

    @task
    def transfrom_countries(df_cube):
        df_cube_country = df_cube[['event_date', 'country', 'likes']]\
            .groupby(['event_date', 'country'])\
            .sum()\
            .reset_index()
        return df_cube_country

    @task
    def load(df_cube_source, df_cube_country):
        print('Likes per source')
        print(df_cube_source.to_csv(index=False, sep='\t'))
        print('Likes per country')
        print(df_cube_country.to_csv(index=False, sep='\t'))

    df_cube = extract()
    df_cube_source = transfrom_source(df_cube)
    df_cube_country = transfrom_countries(df_cube)
    load(df_cube_source, df_cube_country)

dag_sim_example = dag_sim_example()




