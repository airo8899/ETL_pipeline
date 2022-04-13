# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def ch_get_df(query, names, host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t', names=names)
    return result


# Default parameters for tasks
default_args = {
    'owner': 'i-orlov-5',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 10),
}

# DAG run interval
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_rep():
    feed_query = """
        select user_id as id,
               toDate(time) as event_date,
               gender,
               age,
               os,
               source,
               countIf(action='like') as likes,
               countIf(action='view') as views
        from simulator_20220320.feed_actions
        where event_date = today() - 1
        group by id,
               event_date,
               gender,
               age,
               os,
               source
        """
    feed_colnames = ('id', 'event_date', 'gender', 'age', 'os', 'source', 'likes', 'views')

    msg_query = """
        select id, event_date, messages_sent, messages_received, users_sent, users_received,
                       gender,
                       age,
                       os,
                       source
        from
        (
            select * from
            (
                SELECT user_id as id,
                   toDate(time) as event_date,
                   count() as messages_sent,
                   uniqExact(reciever_id) as users_sent,
                   gender,
                   age,
                   os,
                   source
                from simulator_20220320.message_actions
                where event_date = today() - 1
                group by id,
                         event_date,
                         gender,
                         age,
                         os,
                         source
            ) sender
            full outer join
            (
              select inn1.id, event_date, messages_received, users_received,
                     gender,
                     age,
                     os,
                     source
              from
                (
                  SELECT reciever_id as id,
                         toDate(time) as event_date,
                         count() as messages_received,
                         uniqExact(user_id) as users_received
                  from simulator_20220320.message_actions
                  where event_date = today() - 1
                  group by id, event_date
                ) inn1
                join
                (
                  SELECT distinct user_id as id,
                     toDate(time) as event_date,
                     gender,
                     age,
                     os,
                     source
                  from simulator_20220320.message_actions
                  where event_date = today() - 1
                ) inn2
                using(id, event_date)
            ) reciever
            using(id, event_date, gender, age, os, source)
        )
        """
    msg_colnames = ('id', 'event_date', 'messages_sent', 'messages_received', 'users_sent', 'users_received', 'gender', 'age', 'os', 'source')
    

    @task
    def extract_data(query, colnames):
        return ch_get_df(query, colnames)
    
    @task
    def join_dfs(feed_df, msg_df):
        return feed_df.merge(msg_df, how='outer', on=['id','event_date', 'gender', 'age', 'os', 'source'])
    
    @task
    def transform_metric(df, metric_name):
        val_names = ('views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent')
        res = (
            df[['event_date', metric_name, *val_names]]
            .groupby(['event_date', metric_name], as_index=False).sum()
            .rename(columns={metric_name: 'metric_value'})
        )
        res.insert(1, 'metric', metric_name)
        return res
    
    @task
    def load(*args):
        df = pd.concat(args).reset_index().drop('index', axis=1)
        context = get_current_context()
        print(f"""Res {context['ds']}""")
        print(df.to_csv(index=False, sep='\t'))
        
        # ph.to_clickhouse(df, table='test', connection=connection)
        
    feed_df = extract_data(feed_query, feed_colnames)
    msg_df = extract_data(msg_query, msg_colnames)
    
    merged_df = join_dfs(feed_df, msg_df)
    
    os_df = transform_metric(merged_df, 'os')
    gender_df = transform_metric(merged_df, 'gender')
    age_df = transform_metric(merged_df, 'age')
    
    load(os_df, gender_df, age_df)

dag_rep = dag_rep()
