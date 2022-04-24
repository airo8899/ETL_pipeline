from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task

# параметры для airflow
default_args = {
    'owner': 'an-shishkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 21)
}
schedule_interval = '1 10 * * *'

# параметры для clickhouse
connection_from = {
    'host': 'https://clickhouse.lab.karpov.courses/',
    'database':'simulator_20220320',
    'user':'student', 
    'password':'dpo_python_2020'
}
connection_to = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

# sql-запросы
create_table_v1 = """
create table if not exists test.avs_v1
(
    event_date Date,
    gender String,
    age String,
    os String,
    views Int32,
    likes Int32,
    messages_sent Int32,
    users_sent Int32,
    messages_received Int32,
    users_received Int32
)
engine = MergeTree
order by event_date
"""
create_table_v2 = """
create table if not exists test.avs_v2
(
    event_date Date,
    metric String,
    metric_value String,
    views Int32,
    likes Int32,
    messages_sent Int32,
    users_sent Int32,
    messages_received Int32,
    users_received Int32
)
engine = MergeTree
order by event_date
"""
query_actions = '''
select
toDate(time) as event_date,
user_id,
gender,
age,
os,
countIf(action='view') as views,
countIf(action='like') as likes
from simulator_20220320.feed_actions
where toStartOfDay(time) = yesterday()
group by event_date, user_id, gender, age, os
'''
query_messages = '''
with
t1 as (
    select
    toDate(time) as event_date,
    user_id,
    count(user_id) as sent,
    uniqExact(reciever_id) as recievers
    from simulator_20220320.message_actions
    where toStartOfDay(time) = yesterday()
    group by event_date, user_id
),
t2 as (    
    select
    toDate(time) as event_date,
    reciever_id,
    count(reciever_id) as recieved,
    uniqExact(user_id) as senders
    from simulator_20220320.message_actions
    where toStartOfDay(time) = yesterday()
    group by event_date, reciever_id
),
interim as (
    select
    if(t1.event_date = '1970-01-01', t2.event_date, t1.event_date) as event_date,
    if(t1.user_id = 0, t2.reciever_id, t1.user_id) as user_id,
    t1.sent as messages_sent,
    t1.recievers as users_sent,
    t2.recieved as messages_received,
    t2.senders as users_received
    from t1 full join t2 on t1.user_id = t2.reciever_id
),
attr as (
select distinct user_id, gender, age, os
from simulator_20220320.message_actions
)
select
    l.event_date,
    l.user_id,
    r.gender,
    r.age,
    r.os,
    l.messages_sent,
    l.users_sent,
    l.messages_received,
    l.users_received
from interim as l
left join attr as r
on l.user_id = r.user_id
'''

# преобразование возраста
def age_group(age):
    if age < 18:
        return '0-17'
    elif age < 25:
        return '18-24'
    elif age < 41:
        return '25-40'
    elif age < 56:
        return '31-55'
    else:
        return '56+'

# преобразование пола
def gender_str(x):
    if x == 1:
        return 'male'
    else:
        return 'female'

# группировка по метрикам
def calc_aggs_by_metric(tbl, metric):
    tbl['metric'] = metric
    aggs = tbl.groupby(by=['event_date', 'metric', metric])[['views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']].sum().reset_index()
    aggs.rename(columns={metric:'metric_value'},inplace=True)
    return aggs

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def avs_dag():
    '''
    avs_v1 - группировка одновременно в разрезах gender, age, os
    avs_v2 - группировка параллельно в разрезах gender, age, os с последующей конкатенацией
    Так как пункты задания противоречат в части того, как должна выглядеть финальная таблица
    '''
    
    @task
    def hello():
        print('started')
    
    @task
    def check_or_create_table_v1():
        if ph.read_clickhouse(query='exists test.avs_v1', connection=connection_to).iloc[0, 0] == 0:
            ph.execute(connection=connection_to, query=create_table_v1)
            print('avs_v1 was created')
        else:
            print('avs_v1 already exists')
    
    @task
    def check_or_create_table_v2():
        if ph.read_clickhouse(query='exists test.avs_v2', connection=connection_to).iloc[0, 0] == 0:
            ph.execute(connection=connection_to, query=create_table_v2)
            print('avs_v2 was created')
        else:
            print('avs_v2 already exists')
    
    @task
    def extract_actions():
        actions = ph.read_clickhouse(query_actions, connection=connection_from)
        return actions
    
    @task
    def extract_messages():
        messages = ph.read_clickhouse(query_messages, connection=connection_from)
        return messages
    
    @task
    def merge_extracts(actions, messages):
        df = pd.merge(actions, messages, on=['event_date', 'user_id', 'gender', 'age', 'os'], how='outer')
        df['age'] = df['age'].apply(age_group)
        df['gender'] = df['gender'].apply(gender_str)
        df.fillna(0, inplace=True)
        return df
    
    @task
    def group_by_gender(df):
        gender_aggs = calc_aggs_by_metric(tbl=df, metric='gender')
        return gender_aggs
    
    @task
    def group_by_age(df):
        age_aggs = calc_aggs_by_metric(tbl=df, metric='age')
        return age_aggs
    
    @task
    def group_by_os(df):
        os_aggs = calc_aggs_by_metric(tbl=df, metric='os')
        return os_aggs
    
    @task
    def concat_to_table_v2(*args):
        return pd.concat(args).reset_index(drop=True)
    
    @task
    def transform_to_table_v1(df):
        return df.groupby(['event_date', 'gender', 'age', 'os'])[['views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']].sum().reset_index()
    
    @task
    def upload_table_v1(df):
        print(df.to_csv(index=False, sep='\t'))
        ph.to_clickhouse(df.to_csv(index=False, sep='\t'), table='avs_v1', connection=connection_to, index=False)

    @task
    def upload_table_v2(df):
        print(df.to_csv(index=False, sep='\t'))
        ph.to_clickhouse(df.to_csv(index=False, sep='\t'), table='avs_v2', connection=connection_to, index=False)
    
    hello()
    check_or_create_table_v1()
    check_or_create_table_v2()
    
    actions = extract_actions()
    messages = extract_messages()
    merged = merge_extracts(actions, messages)
    
    gender_aggs = group_by_gender(merged)
    age_aggs = group_by_age(merged)
    os_aggs = group_by_os(merged)
    aggs_v2 = concat_to_table_v2(gender_aggs, age_aggs, os_aggs)
    
    aggs_v1 = transform_to_table_v1(merged)
    
    upload_table_v1(aggs_v1)
    upload_table_v2(aggs_v2)
    
avs_dag = avs_dag()
        
