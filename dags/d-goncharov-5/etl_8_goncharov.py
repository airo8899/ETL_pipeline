import os

os.system('pip install pandahouse')

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'd-goncharov-5',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 11),
}

# Интервал запуска DAG
schedule_interval = '0 8 * * *'

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220320'}

upload_con = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'}


# функции преобразования возртаста и пола
def age_category(x):
    if 0 <= x <= 10:
        return '0-10'
    elif 11 <= x <= 25:
        return '11-25'
    elif 26 <= x <= 50:
        return '26-50'
    else:
        return '51+'
    
def gender_category(x):
    if x == 1:
        return 'male'
    else: 
        return 'female'
    

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_goncharov():    
    
    @task()
    def extract_msg():

        message_query='''
        select toDate(time) as event_date,
            user_id, reciever_id, os, gender, age
        from simulator_20220320.message_actions
        where toDate(time) = today() - 1
        '''

        mes_df_start = ph.read_clickhouse(query=message_query, connection = connection)

        return mes_df_start
    
    @task()
    def transform_msg(mes_df_start):

        mes_df_start['age'] = mes_df_start['age'].apply(age_category)
        mes_df_start['gender'] = mes_df_start['gender'].apply(gender_category)

        messages_received_dict = {}
        for user in mes_df_start['user_id']:
            count = mes_df_start[mes_df_start['reciever_id']==user]['user_id'].count()
            messages_received_dict[user] = count

        messages_received_df = pd.DataFrame.from_dict(messages_received_dict, orient='index').reset_index()
        messages_received_df.columns=['user_id', 'messages_received']

        users_received_dict = {}
        for user in mes_df_start['user_id']:
            count = mes_df_start[mes_df_start['reciever_id']==user]['user_id'].nunique()
            users_received_dict[user] = count  

        users_received_df = pd.DataFrame.from_dict(users_received_dict, orient='index').reset_index()
        users_received_df.columns=['user_id', 'users_received']

        df_msg_user = messages_received_df.merge(users_received_df, on='user_id')
        df_message = mes_df_start.merge(df_msg_user, on='user_id')

        df_message['messages_sent'] = 0

        df_mes_grouped = df_message.groupby('user_id').agg({'event_date':'min', \
                                                            'age':'min', \
                                                            'os':'min', \
                                                            'gender': 'min', \
                                                            'reciever_id':'nunique', \
                                                            'messages_received': 'min', \
                                                            'users_received':'min', \
                                                            'messages_sent': 'count'})  

        df_mes_grouped['users_sent'] = df_mes_grouped['reciever_id']
        df_mes_grouped.drop(columns='reciever_id', inplace=True)

        df_mes_final = df_mes_grouped.reset_index().copy()

        return df_mes_final

    @task()
    def extract_feed():

        feed_query = '''
        select min(toDate(time)) as event_date,
            user_id,
            countIf(action='like') as likes,
            countIf(action='view') as views,
            min(age) as age,
            min(gender) as gender,
            min(os) as os
        from simulator_20220320.feed_actions
        where toDate(time) = today() - 1
        group by user_id
        '''
        df_feed_final = ph.read_clickhouse(query=feed_query, connection = connection)

        return df_feed_final

    @task()
    def transform_feed(df_feed_final):

        df_feed_final['age'] = df_feed_final['age'].apply(age_category)
        df_feed_final['gender'] = df_feed_final['gender'].apply(gender_category)

        return df_feed_final

    @task()
    def feed_msg_merge(df_feed_final, df_mes_final):

        df_merge = df_feed_final.merge(df_mes_final, on=['user_id','gender','os','age', 'event_date'], how='outer').fillna(0)
        return df_merge


    @task()
    def group_os(df_merge):

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


    @task()
    def group_os(df_merge):

        df_gender = df_merge.groupby('gender').agg({'event_date':'min', \
                                'likes':'sum', \
                                'views': 'sum', \
                                'messages_received':'sum', \
                                'users_received':'sum', \
                                'messages_sent':'sum', \
                                'users_sent':'sum'}).reset_index().copy()
        df_gender['metric'] = 'gender'
        df_gender.rename(columns={'gender':'metric_value'},inplace=True)

        return df_gender

    @task()
    def group_age(df_merge):

        df_age = df_merge.groupby('age').agg({'event_date':'min', \
                                'likes':'sum', \
                                'views': 'sum', \
                                'messages_received':'sum', \
                                'users_received':'sum', \
                                'messages_sent':'sum', \
                                'users_sent':'sum'}).reset_index().copy()
        df_age['metric'] = 'age'
        df_age.rename(columns={'age':'metric_value'},inplace=True)

        return df_age

    @task()
    def concat(df_age, df_os, df_gender):

        final = pd.concat([df_gender, df_age, df_os])

        new_cols = ['event_date',
                'metric',
                'metric_value',
                'views',
                'likes',
                'messages_received',
                'messages_sent',
                'users_received',
                'users_sent']  

        final = final.loc[:, new_cols]
        final = final.reset_index().drop(columns='index')
        final = final.astype({'views':'int', \
                   'likes':'int', \
                   'messages_received':'int', \
                   'messages_sent':'int', \
                   'users_received':'int', \
                   'users_sent':'int'})              
        return final


    @task()
    def load(final):

        ph.to_clickhouse(df=final, table='gdv', index=False, connection = upload_con)

    extracted_msg = extract_msg()
    final_msg = transform_msg(extracted_msg)
    extracted_feed = extract_feed()
    final_feed = transform_feed(extracted_feed)
    feed_msg_merged = feed_msg_merge(final_feed, final_msg)
    grouped_os = group_os(feed_msg_merge)
    grouped_gender = group_gender(feed_msg_merge)
    grouped_age = group_age(feed_msg_merge)
    concat_res = concat(grouped_age, grouped_os, grouped_gender)
    loading = load(concat_res) 
    
dag_etl_goncharov = dag_etl_goncharov()    
