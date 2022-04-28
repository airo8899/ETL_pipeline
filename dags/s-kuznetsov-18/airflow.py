from datetime import datetime, timedelta
import pandas as pd

import pandahouse

from airflow.decorators import dag, task

default_args = {
    'owner': 's-kuznetsov-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220320'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_kuznetsov():
    @task
    def counts_feed_metrics():
        query = '''SELECT toDate(time) as event_date,
                          gender,
                          multiIf(age <= 17, 'до 18', age > 17
                           and age <= 30, '18-30', age > 30
                           and age <= 50, '31-50', '50+') as age,
                          os,
                          user_id,
                          sum(action = 'like') as likes,
                          sum(action = 'view') as views
                   FROM simulator_20220320.feed_actions
                   WHERE event_date = yesterday()
                   GROUP BY event_date, user_id, gender, age, os
'''
        df_feed = pandahouse.read_clickhouse(query, connection=connection)
        return df_feed

    @task
    def counts_messenger_metrics():
        query = '''SELECT  event_date,
                           user_id,
                           messages_sent,
                           users_sent,
                           messages_received,
                           users_received
                    FROM
                        (SELECT toDate(time) event_date,  
                                user_id,
                                count() messages_sent,
                                count(distinct reciever_id) users_sent
                         FROM simulator_20220320.message_actions
                         WHERE event_date = yesterday()
                         GROUP BY user_id, event_date) t1
                         LEFT JOIN
                        (SELECT toDate(time) event_date,  
                                reciever_id,
                                count() messages_received,
                                uniqExact(user_id) users_received
                         FROM simulator_20220320.message_actions
                         WHERE event_date = yesterday()
                         GROUP BY reciever_id, event_date) t2
                         ON t1.user_id = t2.reciever_id
'''
        df_message = pandahouse.read_clickhouse(query, connection=connection)
        return df_message

    @task
    def merge_df(df_message, df_feed):
        full_df = pd.merge(df_message, df_feed, on=['user_id', 'event_date'], how='outer')
        return full_df

    @task
    def transfrom_gender(full_df):
        df_gender = full_df.groupby('gender', as_index=False) \
            .agg({'event_date': 'min', \
                  'messages_sent': 'sum', \
                  'users_sent': 'sum', \
                  'messages_received': 'sum', \
                  'users_received': 'sum', \
                  'likes': 'sum', \
                  'views': 'sum'}) \
            .reset_index().copy()
        df_gender['metric'] = 'gender'
        df_gender.rename(columns={'gender': 'metric_value'}, inplace=True)
        return df_gender

    @task
    def transfrom_os(full_df):
        df_os = full_df.groupby('os', as_index=False) \
            .agg({'event_date': 'min', \
                  'messages_sent': 'sum', \
                  'users_sent': 'sum', \
                  'messages_received': 'sum', \
                  'users_received': 'sum', \
                  'likes': 'sum', \
                  'views': 'sum'}) \
            .reset_index().copy()
        df_os['metric'] = 'os'
        df_os.rename(columns={'os': 'metric_value'}, inplace=True)
        return df_os

    @task
    def transfrom_age(full_df):
        df_age = full_df.groupby('age', as_index=False) \
            .agg({'event_date': 'min', \
                  'messages_sent': 'sum', \
                  'users_sent': 'sum', \
                  'messages_received': 'sum', \
                  'users_received': 'sum', \
                  'likes': 'sum', \
                  'views': 'sum'}) \
            .reset_index().copy()
        df_age['metric'] = 'age'
        df_age.rename(columns={'age': 'metric_value'}, inplace=True)
        return df_age

    @task
    def df_concat(df_gender, df_age, df_os):
        concat_table = pd.concat([df_gender, df_age, df_os])
        new_cols = ['event_date', \
                    'metric', \
                    'metric_value', \
                    'views', \
                    'likes', \
                    'messages_received', \
                    'messages_sent', \
                    'users_received', \
                    'users_sent']

        final_df = concat_table.loc[:, new_cols]
        final_df = final_df.reset_index().drop('index', axis=1)
        final_df = final_df.astype({
            'metric': 'str', \
            'metric_value': 'str', \
            'views': 'int', \
            'likes': 'int', \
            'messages_received': 'int', \
            'messages_sent': 'int', \
            'users_received': 'int', \
            'users_sent': 'int'})

        return final_df

    @task
    def load(final_df):
     q = '''
                CREATE TABLE IF NOT EXISTS test.SKuznetsov
                (   event_date Date,
                    metric String,
                    metric_value String,
                    likes UInt64,
                    views UInt64,
                    messages_sent UInt64,
                    users_sent UInt64,
                    messages_received UInt64,
                    users_received UInt64
                ) ENGINE = Log()'''

     pandahouse.execute(connection=connection_test, query=q)

     pandahouse.to_clickhouse(df=final_df, table='SKuznetsov', connection=connection_test, index=False)

     df_feed = counts_feed_metrics()
     df_message = counts_messenger_metrics()
     full_df = merge_df(df_feed, df_message)
     df_gender = transfrom_gender(full_df)
     df_age = transfrom_age(full_df)
     df_os = transfrom_os(full_df)
     final_df = df_concat(df_gender, df_age, df_os)
     load(final_df)


dag_kuznetsov = dag_kuznetsov()

