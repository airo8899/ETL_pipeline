import pandas as pd
import pandahouse
import numpy as np
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os

# установка пандахауса
os.system("pip install pandahouse")

# бд от куда загрузить исходные данные
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220320'
}

# бд куда выгрузить данные
connection_upload = {
                'host': 'https://clickhouse.lab.karpov.courses',
                'password': '656e2b0c9c',
                'user': 'student-rw',
                'database': 'test'
}

# функция запроса к бд
def select(q):
    return pandahouse.read_clickhouse(q, connection=connection)

# настройки айрофлоу
default_args = {
    'owner': 'a-dijkov-5',
    'depends_on_past': False,
    'retries': 22,
    'retry_delay': timedelta(minutes=55),
    'start_date': datetime(2022, 4, 20),
}

# настройки планиривощка
schedule_interval = '0 0 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_diykov():
    
    # запрос для получения данных по ленте новостей
    @task
    def extract_feed():
        return select("""
                SELECT toDate(time) event_date,
                       user_id, gender, os, age,
                       countIf(action, action='like') likes,
                       countIf(action, action='view') views
                FROM simulator_20220320.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY user_id, gender, os, age, event_date""")

    # запрос для получения данных по сервису сообщений
    @task
    def extract_message():
        return select("""
                SELECT l.date event_date, user_id, gender, os, age, 
                       messages_sent, 
                       users_sent,
                       messages_received,
                       users_received
                FROM
                (SELECT toDate(time) date,
                      user_id, gender, os, age,
                      COUNT(reciever_id) messages_sent,
                      COUNT(DISTINCT reciever_id) users_sent
                FROM simulator_20220320.message_actions
                WHERE toDate(time) = yesterday()
                GROUP BY user_id, gender, os, age, date) l

                INNER JOIN

                (SELECT reciever_id, toDate(time) date,
                      COUNT(user_id) messages_received,
                      COUNT(DISTINCT user_id) users_received
                FROM simulator_20220320.message_actions
                WHERE toDate(time) = yesterday()
                GROUP BY reciever_id, date) r

                ON l.user_id = r.reciever_id AND l.date= r.date""")

    # функция объединия двух запросов 
    @task
    def merge_df(feed_df, message_df):
        
        # смержим обе таблицы
        df = feed_df.merge(message_df, on=['event_date', 'user_id'], how='outer')

        # сделаем единные колонки пол, платформа, возраст
        # если значение в колонке не равно себе (x['..._x']!=x['..._x']), то это значение Nan
        df['gender'] = df.apply(lambda x: x['gender_x'] if x['gender_x']==x['gender_x'] else x['gender_y'], axis=1)
        df['os'] = df.apply(lambda x: x['os_x'] if x['os_x']==x['os_x'] else x['os_y'], axis=1)
        df['age'] = df.apply(lambda x: x['age_x'] if x['age_x']==x['age_x'] else x['age_y'], axis=1)

        # замена пола c интового значения на 'male' и 'female'
        df['gender'] = df['gender'].apply(lambda x: 'male' if x==1 else 'female')

        # замена возраста на категории
        def age_category(age):
            if age < 18:
                return '0-17'
            elif age < 25:
                return '18-24'
            elif age < 31:
                return '25-30'
            elif age < 46:
                return '31-45'
            elif age < 61:
                return '46-60'
            else:
                return '60+'

        df['age'] = df['age'].apply(age_category)

        # отберем необходимые столбцы
        df = df[['event_date', 'gender', 'os', 'age', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']]

        return df

    
    # функция для построения сводной таблицы, где мультиндекс - дата, пол, платформа, возраст, 
    # колонки - небходимые метрики 
    @task
    def transform_miltigroup(df):
        multigroup = df.groupby(['event_date', 'gender', 'os', 'age'], as_index=False) \
            [['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']].sum()
        multigroup[['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']] = \
            multigroup[['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']].astype(int)
        return multigroup

    

    # функция для загрузки данных в таблице diykov_v1 бд test,
    # если таблицы не существует, то она будет создана 
    @task
    def load_to_test1(multigroup):
        q = '''
                CREATE TABLE IF NOT EXISTS test.diykov_v1
                (   event_date Date,
                    gender String,
                    os String,
                    age String,
                    likes UInt64,
                    views UInt64,
                    messages_sent UInt64,
                    users_sent UInt64,
                    messages_received UInt64,
                    users_received UInt64
                ) ENGINE = Log()'''

        pandahouse.execute(connection=connection_upload, query=q)


        pandahouse.to_clickhouse(df=multigroup, table='diykov_v1', connection=connection_upload, index=False)

    
    
    # функция для создания сводной таблицы по метрике
    def group(df, metric):
        df_group = df.groupby(metric)[['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']].sum().reset_index()
        df_group.insert(0, 'metric', f'{metric}')
        df_group.insert(0, 'event_date', df['event_date'][0])
        df_group.columns = ['event_date', 'metric', 'metric_value', 'likes', 'views', 'messages_sent',
               'users_sent', 'messages_received', 'users_received']
        return df_group


    # свобная таблица по полу
    @task
    def transform_gender(df):
        df_gender = group(df, 'gender')
        return df_gender
    
    # свобная таблица по платформе
    @task    
    def transform_os(df):
        df_os = group(df, 'os')
        return df_os
    
    # свобная таблица по возрасту
    @task
    def transform_age(df):
        df_age = group(df, 'age')
        return df_age
    
    # объединие трех сводных таблиц во одну
    @task    
    def concat_df(df_gender, df_os, df_age):
        separategroup = pd.concat([df_gender, df_os, df_age])
        separategroup[['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']] = \
                separategroup[['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']].astype(int)
        return separategroup
        
    # функция для загрузки данных в таблице diykov_v2 бд test,
    # если таблицы не существует, то она будет создана 
    @task
    def load_to_test2(separategroup):
        q = '''
                CREATE TABLE IF NOT EXISTS test.diykov_v2
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

        pandahouse.execute(connection=connection_upload, query=q)
        
        pandahouse.to_clickhouse(df=separategroup, table='diykov_v2', connection=connection_upload, index=False)
        
        
        

    feed_df = extract_feed()
    message_df = extract_message()
    df = merge_df(feed_df, message_df)
    
    multigroup = transform_miltigroup(df)
    # print(multigroup)
    load_to_test1(multigroup)

    df_gender = transform_gender(df)
    df_os = transform_os(df)
    df_age = transform_age(df)
    separategroup = concat_df(df_gender, df_os, df_age)
    # print(separategroup)
    load_to_test2(separategroup)
   
dag_etl_diykov = dag_etl_diykov()