from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


connection_upload = {'host': 'https://clickhouse.lab.karpov.courses',
        'password': '656e2b0c9c',
        'user': 'student-rw',
        'database': 'test'}                     

def ch_get_df(query='Select * ', connection = connection_upload):
    r = requests.post(host, data=query.encode("utf-8"), auth=(connection['user'], connection['password']), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


connection_read = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }
def ch_get_df2(query , connection):
    df = ph.read_clickhouse(query = query, connection=connection)
    return df

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.poptsov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 21),
}

# Интервал запуска DAG
schedule_interval = '0 20 * * *'

calc_columns_list = ['likes','views','send_messages','to_users','receive_messages','from_users']

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_poptsov():

    @task()
    def extract_feed_actions():
        query = """
        SELECT toDate(time) AS event_date,
           user_id,
           gender,
           age,
           os,
           countIf(user_id, action = 'like') as likes,
           countIf(user_id, action = 'view') as views
        FROM simulator_20220320.feed_actions
        WHERE toDate(time) = '2022-04-21'
        GROUP BY 
            event_date,
            user_id,
            gender,
            age,
            os"""       
        df = ch_get_df2(query=query, connection = connection_read)
        return df    

    @task()
    def extract_message_actions():
        query = """
        select *
        from 
        (SELECT toDate(time) AS event_date,
               user_id, 
               gender,
               age,
               os,
               count() as send_messages,
               count(distinct reciever_id) as to_users
        FROM simulator_20220320.message_actions
        WHERE toDate(time) = '2022-04-21'
        GROUP BY 
            event_date,
            user_id,
           gender,
           age,
           os) t1
        JOIN

        (SELECT reciever_id, 
           count() as receive_messages,
           count(distinct user_id) as from_users
        FROM simulator_20220320.message_actions
        WHERE toDate(time) = '2022-04-21'
        GROUP BY 
            toDate(time),
            reciever_id) t2
        ON t1.user_id = t2.reciever_id """       
        
        df = ch_get_df2(query=query, connection = connection_read)
        df = df.drop('reciever_id', axis = 1)
        return df
    
    
    @task()
    def merge(df_feed_actions, df_message_actions):
        df = df_feed_actions.merge(df_message_actions, on = ['event_date', 'user_id', 'gender', 'age', 'os'], how = 'left').fillna(0)
        return df

    @task()
    def calc_group_metric(df, group_column):
        full_columns_list = ['event_date']+calc_columns_list + group_column    
        df = df[full_columns_list].groupby(by = ['event_date']+group_column).sum().reset_index()
        df['metric'] = group_column[0]
        df = df.rename({group_column[0]:'metric_values'}, axis = 1)
        return df    
    
    @task()
    def load(df_os, df_age, df_gender):
        context = get_current_context()
        ds = context['ds']
        print(f'Current date {ds}')
        print('metric OS')
        print(df_os.head())
        print('metric AGE')
        print(df_age.head())
        print('metric GENDER')
        print(df_gender.head())
    
        df_result = pd.concat([df_os, df_age, df_gender], axis = 0)
        
        q = '''
                CREATE TABLE IF NOT EXISTS test.apoptsov
                (   event_date Date,
                    metric String,
                    metric_values String,
                    likes UInt64,
                    views UInt64,
                    send_messages UInt64,
                    to_users UInt64,
                    receive_messages UInt64,
                    from_users UInt64
                ) ENGINE = Log()'''
        ph.execute(connection=connection_upload, query=q)        
    
        ph.to_clickhouse(df = df_result, table='apoptsov', index=False, connection = connection_upload)

    df_feed_actions = extract_feed_actions()
    df_message_actions = extract_message_actions()
    df = merge(df_feed_actions, df_message_actions)
    
    df_os = calc_group_metric(df, ['os'])
    df_age = calc_group_metric(df, ['age'])
    df_gender = calc_group_metric(df, ['gender'])
    
    load(df_os, df_age, df_gender)

# New DAG name     
poptsov_dag = dag_poptsov()
