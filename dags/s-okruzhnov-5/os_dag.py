from pip._internal import main as pipmain
pipmain(['install', 'pandahouse'])
import pandahouse
import pandahouse
from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
from tabulate import tabulate

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from pip._internal import main as pipmain

def ch_get_df(query, host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

default_args = {
    'owner': 'sokruzhnov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 11),
}

schedule_interval = '0 9 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl():
    feed_query = """SELECT toDate(time) event_date,
                           user_id,
                           age,
                           os,
                           gender,
                           sum(action = 'like') likes,
                           sum(action = 'view') views
                    FROM simulator_20220320.feed_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY event_date,
                             user_id,
                             age,
                             os,
                             gender
                             format TSVWithNames"""
    
    
    msg_query = """SELECT t1.event_date,
                          t1.user_id,
                          t1.users_received,
                          t2.users_sent,
                          t1.messages_sent,
                          t2.messages_received
                    FROM
                    (SELECT toDate(time) event_date,
                            user_id,
                            count(distinct reciever_id) users_received,
                            count(*) messages_sent
                    FROM simulator_20220320.message_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY user_id, event_date) t1
                    LEFT JOIN 
                    (SELECT reciever_id,
                            count(distinct user_id) users_sent,
                            count(*) messages_received
                    FROM simulator_20220320.message_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY reciever_id) t2
                    on t1.user_id = t2.reciever_id
                    format TSVWithNames"""
    
    @task
    def extract_data(query):
        return ch_get_df(query)
    
    @task
    def merge_df(feed, msg):
        return feed.merge(msg, on=['user_id', 'event_date'] , how='outer')
    
    @task
    def transform_metric(df, metric_value):
        columns = ('likes', 'views', 'users_received', 'users_sent', 'messages_sent', 'messages_received')
        res = (
            df[['event_date', metric_value, *columns]]
            .groupby(['event_date', metric_value], as_index=False).sum()
            .rename(columns={metric_value: 'metric_value'})
        )
        res.insert(1, 'metric', metric_value)
        return res
    
    @task
    def load(*args):
        df = pd.concat(args).reset_index().drop('index', axis=1)
        context = get_current_context()
        ds = context['ds']
        print(f"""Metrics for {ds}""")
        print(df.to_csv(index=False, sep='\t'))
        connection = {
                        'host': 'https://clickhouse.lab.karpov.courses',
                        'password': 'dpo_python_2020',
                        'user': 'student',
                        'database': 'test'
        }
        pandahouse.to_clickhouse(df, 'sokruzhnov_test', connection=connection)
        
        
    feed = extract_data(feed_query)
    msg = extract_data(msg_query)
    merged_df = merge_df(feed, msg)
    
    os_df = transform_metric(merged_df, 'os')
    gender_df = transform_metric(merged_df, 'gender')
    age_df = transform_metric(merged_df, 'age')
    
    load(os_df, gender_df, age_df)
    
dag_etl = dag_etl()