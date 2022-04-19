from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
from pandahouse.http import execute
from pandahouse.core import to_clickhouse, read_clickhouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220320'}


 # Дефолтные параметры
default_args = {
    'owner': 'e-sentjurina-5',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 14),
}

connect = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'}


# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_sentyurina():
    
    @task
    # формируем датафрейм из ленты новостей
    def extract_feed():
        query_feed = """SELECT toDate(time) event_date, 
                                user_id,
                                gender,
                                age,
                                os, 
                                countIf(action='like')  likes,
                                countIf(action='view') as views
                            FROM simulator_20220320.feed_actions
                            WHERE event_date = today() - 1
                            GROUP BY event_date,
                                    user_id, 
                                    gender,
                                    age,
                                    os
                             """
        df_feed = ph.read_clickhouse(query=query_feed, connection = connection)
        return df_feed
    
    @task
    # формируем датафрейм по сообщениям
    def extract_msg():
        query_msg = """SELECT event_date,
                                user_id,
                                messages_sent,
                                users_sent,
                                messages_recieved,
                                users_received
                    FROM
                        (SELECT toDate(time) event_date,  
                                user_id,
                                count() messages_sent,
                                uniqExact(reciever_id) users_sent
                            FROM simulator_20220320.message_actions
                            WHERE event_date = today() - 1
                            GROUP BY user_id, event_date) t1
                        JOIN
                        (SELECT toDate(time) event_date, reciever_id, 
                                count() messages_recieved,
                                count(distinct user_id) users_received
                            FROM simulator_20220320.message_actions
                            WHERE event_date = today() - 1
                            GROUP BY reciever_id, event_date) t2
                            ON t1.user_id = t2.reciever_id
                            """
        
        df_msg = ph.read_clickhouse(query=query_msg, connection = connection)
        return df_msg
    
    @task
    # объединение двух датафреймов
    def merge_df(df_feed, df_msg): 
        msg_and_feed = df_feed.merge(df_msg, on=['event_date', 'user_id'] , how='outer')
        return msg_and_feed
    
    @task
    # агрегируем данные по полу
    def transform_gender(df_merge):
        
        # преобразуем пол в читабельный вид 
        def gender_category(x):
            if x == 1:
                return 'male'
            else: 
                return 'female'

        df_gender = df_merge.groupby('gender').agg({'event_date':'min', \
                            'likes':'sum', \
                            'views': 'sum', \
                            'messages_received':'sum', \
                            'users_received':'sum', \
                            'messages_sent':'sum', \
                            'users_sent':'sum'}).reset_index().copy()
        df_gender['metric'] = 'gender'
        df_gender['gender'] = df_gender.gender.apply(gender_category)
        df_gender.rename(columns={'gender':'metric_value'},inplace=True)

        return df_gender
    
    @task
    # агрегируем данные по возрасту
    def transform_age(df_merge): 
        
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
        
        df_age = df_merge.groupby('age').agg({'event_date':'min', \
                            'likes':'sum', \
                            'views': 'sum', \
                            'messages_received':'sum', \
                            'users_received':'sum', \
                            'messages_sent':'sum', \
                            'users_sent':'sum'}).reset_index().copy()
        df_age['metric'] = 'age'
        df_age['age'] = df_age.age.apply(age_category)
        df_age.rename(columns={'age':'metric_value'},inplace=True)

        return df_age

    @task
    # агрегируем данные по платформе
    def transform_os(df_merge):
        df_os = df_merge.groupby('os').agg({'event_date':'min', \
                            'likes':'sum', \
                            'views': 'sum', \
                            'messages_received':'sum', \
                            'users_received':'sum', \
                            'messages_sent':'sum', \
                            'users_sent':'sum'}).reset_index().copy()
        df_os['metric'] = 'os'
        df_os.rename(columns={'os':'metric_value'},inplace=True)

        return df_os
    
    @task
    # соединяем полученные в ходе агрегаций датафреймы
    def df_concat(df_gender, df_age, df_os):

        final_table = pd.concat([df_gender, df_age, df_os])

        new_cols = ['event_date',
                'metric',
                'metric_value',
                'views',
                'likes',
                'messages_received',
                'messages_sent',
                'users_received',
                'users_sent']  

        final_table = final_table.loc[:, new_cols]
        final_table = final_table.reset_index().drop(columns='index')
        final_table = final_table.astype({'views':'int', \
                   'likes':'int', \
                   'messages_received':'int', \
                   'messages_sent':'int', \
                   'users_received':'int', \
                   'users_sent':'int'})              
        return final_table
    
    @task
    def load(fina_tablel):

        ph.to_clickhouse(df=final_table, table='sentyurina', index=False, connection = connect)

    feed = extract_feed()
    msg = extract_msg()
    feed_msg = merge_df(feed, msg)
    gender = transform_gender(feed_msg)
    age = transform_age(feed_msg)
    os = transform_os(feed_msg)
    final = df_concat(gender, age, os)
    load(final)
    
dag_sentyurina = dag_sentyurina()


    

        
    
    
    
