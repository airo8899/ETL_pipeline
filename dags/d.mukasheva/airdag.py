from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}



def querry(q):
    df = pandahouse.read_clickhouse(q, connection=connection)
    return df

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'd.mukasheva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_dina():

    @task()
    def feed():
        q='''
        select
        toDate(time) as event_date, 
        user_id as user,
        gender,
        age,
        os,
        countIf(action, action='like') likes,
        countIf(action, action='view') views
        from simulator_20220320.feed_actions  
        where toDate(time)>=yesterday()
        group by event_date,user,gender,age,os'''
        
        df_feed = pandahouse.read_clickhouse(q, connection=connection)
        return df_feed
        
    @task()
    def messages(): 
        q='''
        with sent as (
        select 
        toDate(time) as event_date,
        user_id as user,
        gender,
        age,
        os,
        count(distinct reciever_id) as sent_users,
        count(reciever_id) as sent_messages
        from simulator_20220320.message_actions
        where event_date = yesterday() 
        group by event_date,user,gender,age,os
        ),
        received as (
        select 
        toDate(time) as event_date,
        reciever_id as user,
        count(distinct user_id) as received_users,
        count(user_id) as received_messages
        from simulator_20220320.message_actions
        where event_date = yesterday() 
        group by event_date,user

        )
        select 
        a.event_date,
        a.user,
        gender,
        age,
        os,
        sent_users,
        sent_messages,
        received_users,
        received_messages
        from sent a inner join received b on a.user=b.user '''
        
        df_messages = pandahouse.read_clickhouse(q, connection=connection)
        return df_messages
    
    @task()
    def merge(df_feed, df_messages): 
        df = pd.merge(df_feed, df_messages, on=['event_date','user','gender','age','os'],how = 'outer')
        return df
    
    @task()
    def fintable(df):
        df_fin = df[['event_date', 'gender', 'age', 'os', 'views', 'likes', 
                          'received_messages', 'sent_messages', 'received_users', 'sent_users']]\
            .groupby(['event_date', 'gender', 'age', 'os'], as_index=False)\
            .sum()
        df_fin[['likes', 'views', 'sent_messages', 'sent_users', 'received_messages', 'received_users']] = \
        df_fin[['likes', 'views', 'sent_messages', 'sent_users', 'received_messages', 'received_users']].astype(int)
        return df_fin
    
    @task
    def load(df_fin):
        context = get_current_context()
        ds = context['ds']
        print(f'Likes per source for {ds}')
        print(df_fin.to_csv(index=False, sep='\t'))
       
    df_feed = feed()
    df_messages=messages()
    df=merge(df_feed, df_messages)
    df_fin=fintable(df)

dag_dina = dag_dina()
