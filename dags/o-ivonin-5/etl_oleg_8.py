#coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

class Getch:
    def __init__(self, query, db='simulator_20220320'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = ph.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)




# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'o-ivonin-5',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 20),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def etl_oleg():
    
    #загрузка данных
    @task
    def extract_feed():
        query_feed = Getch ("""SELECT user_id AS id,
               toDate(time) AS event_date,gender,age,os,countIf(action='like') AS likes,countIf(action='view') AS views
        FROM simulator_20220320.feed_actions
        WHERE event_date = today() - 1
        GROUP BY id,event_date,gender,age,os
        """).df
        return(query_feed)
    #загрузка данных
    @task
    def extract_message():   
        query_message = Getch ("""
        select user,event_date,gender,age,os,messages_received,messages_sent,users_received,users_sent
        from
        
        (select reciever_id as user,toDate(time) as event_date, count(reciever_id) as messages_received, count(distinct user_id) as users_received
        FROM simulator_20220320.message_actions
        WHERE toDate(time) = today() - 1
        group by user, event_date
        ) t1

        full outer join

        (select user_id as user,toDate(time) as event_date, count(user_id) as messages_sent, count(distinct reciever_id) as users_sent,gender,age,os,source
        FROM simulator_20220320.message_actions
        WHERE toDate(time) = today() - 1
        group by user, event_date,gender,age,os,source
        ) t2

        on t1.user = t2.user and t1.event_date = t2.event_date
       """).df
        return(query_message)
        
    feed_data = extract_feed()
    message_data = extract_message()
    

etl_oleg = etl_oleg()
