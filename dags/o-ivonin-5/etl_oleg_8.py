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




# Дефолтные параметры, которые прокидываются в таски qawedawdawda
default_args = {
    'owner': 'o-ivonin-5',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 20),
}

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def etl_oleg():
    
    #загрузка данных
    @task
    def extract_feed():
        query_feed = Getch ("""SELECT DISTINCT user_id AS user,toDate(time) AS event_date,gender,age,os,countIf(action='like') AS likes,countIf(action='view') AS views
        FROM simulator_20220320.feed_actions
        WHERE event_date = today() - 1
        GROUP BY user,event_date,gender,age,os
        """).df
        
        return(query_feed)
    #загрузка данных
    
    @task
    def extract_message():   
        query_message = Getch ("""
        select user,event_date,messages_received,messages_sent,users_received,users_sent
        from
        
        (select reciever_id as user,toDate(time) as event_date, count(reciever_id) as messages_received, count(distinct user_id) as users_received
        FROM simulator_20220320.message_actions
        WHERE toDate(time) = today() - 1
        group by user, event_date
        ) t1

        join

        (select user_id as user,toDate(time) as event_date, count(user_id) as messages_sent, count(distinct reciever_id) as users_sent
        FROM simulator_20220320.message_actions
        WHERE toDate(time) = today() - 1
        group by user, event_date
        ) t2

        on t1.user = t2.user and t1.event_date = t2.event_date
       """).df
        return(query_message)
    
    @task
    def feed_message(query_message,query_feed): 
        msg_and_feed = query_feed.merge(query_message, on=['event_date', 'user'] , how='outer')
        return (msg_and_feed)
    
    @task
    #gender table
    def transform_gender(msg_and_feed):
        def gender_category(x):
            if x == 1:
                return 'male'
            else: 
                return 'female'

        msg_and_feed['gender'] = msg_and_feed['gender'].apply(gender_category)
        df_gender = msg_and_feed.groupby('gender').agg({'event_date':'max', \
                                    'likes':'sum', \
                                    'views': 'sum', \
                                    'messages_received':'sum', \
                                    'users_received':'sum', \
                                    'messages_sent':'sum', \
                                    'users_sent':'sum'}).reset_index().copy()

        df_gender['metric'] = 'gender'
        df_gender.rename(columns={'gender':'value'},inplace=True)
        return df_gender
    
    @task
    #age table
    def transform_age(msg_and_feed): 
        
        # преобразуем возраст в категории
        def age_category(x):
            if 0 <= x <= 20:
                return '0-20'
            elif 21 <= x <= 30:
                return '21-30'
            elif 31 <= x < 50:
                return '31-50'
            else:
                return '50+'
        
        msg_and_feed['age'] = msg_and_feed['age'].apply(age_category)

        df_age = msg_and_feed.groupby('age').agg({'event_date':'min', \
                                    'likes':'sum', \
                                    'views': 'sum', \
                                    'messages_received':'sum', \
                                    'users_received':'sum', \
                                    'messages_sent':'sum', \
                                    'users_sent':'sum'}).reset_index().copy()

        df_age['metric'] = 'age'
        df_age.rename(columns={'age':'value'},inplace=True)
        return df_age
    @task
    #os table
    def transform_os(msg_and_feed):
        df_os = msg_and_feed.groupby('os').agg({'event_date':'min', \
                                    'likes':'sum', \
                                    'views': 'sum', \
                                    'messages_received':'sum', \
                                    'users_received':'sum', \
                                    'messages_sent':'sum', \
                                    'users_sent':'sum'}).reset_index().copy()
        df_os['metric'] = 'os'
        df_os.rename(columns={'os':'value'},inplace=True)
        return df_os
    @task
    def df_concat(df_gender, df_age, df_os):
        concat_table = pd.concat([df_gender, df_age, df_os])
        new_cols = ['event_date',
                    'metric',
                    'value',
                    'views',
                    'likes',
                    'messages_received',
                    'messages_sent',
                    'users_received',
                    'users_sent']  

        final_table = concat_table.loc[:, new_cols]
        final_table = final_table.reset_index().drop('index', axis =1)
        final_table['event_date'] = final_table['event_date'].apply(lambda x: datetime.isoformat(x))
        final_table = final_table.astype({
                        'metric':'str',
                        'value':'str',  
                        'views':'int', \
                        'likes':'int', \
                        'messages_received':'int', \
                        'messages_sent':'int', \
                        'users_received':'int', \
                        'users_sent':'int'}) 

        return final_table
    @task
    def load(final_table):
        ph.to_clickhouse(df=final_table, table='Metrics_Oleg', index=False, \
                         connection = connection)


    feed = extract_feed()
    msg = extract_message()
    feed_msg = feed_message(feed, msg)
    gender = transform_gender(feed_msg)
    age = transform_age(feed_msg)
    os = transform_os(feed_msg)
    final_table = df_concat(gender, age, os)
    load(final_table)

etl_oleg = etl_oleg()
