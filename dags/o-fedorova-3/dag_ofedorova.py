from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
from pandahouse.core import to_clickhouse, read_clickhouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'o-fedorova-3',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '0 8 * *'


connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220320'}

connection1 = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_ofedorova():
    
    @task
    # В feed_actions для каждого юзера посчитаем число просмотров и лайков контента
    def extract_feed():
        query_feed = """SELECT
            user_id as id,
            toDate(time) as event_date,
            gender,
            age,
            os,
            source,
            countIf(action='like') as likes,
            countIf(action='view') as views
            from simulator_20220320.feed_actions
            where event_date = today() - 1
            group by 
            id,
            event_date,
            gender,
            age,
            os,
            source
            """

        df_cube_feed = ph.read_clickhouse(query=query_feed, connection = connection)

        return df_cube_feed
    
    @task
    ##В message_actions для каждого юзера считаем, сколько он получает и отсылает
    ##сообщений, скольким людям он пишет, сколько людей пишут ему.

    def extract_messages():
        query_messages= """SELECT
            id,
            event_date,
            messages_sent,
            users_sent,
            messages_recieved,
            users_received
            FROM
            (SELECT user_id as id,
            toDate(time) as event_date,
            count() as messages_sent,
            uniqExact(reciever_id) as users_sent
            from simulator_20220320.message_actions
            where event_date = today() - 1
            group by id, event_date) sent
            JOIN
            (SELECT reciever_id as id,
            toDate(time) as event_date,
            count() as messages_recieved,
            count(distinct user_id) as users_received
            from simulator_20220320.message_actions
            where event_date = today() - 1
            group by id, event_date) recieved
            using id
            """
        df_cube_message = ph.read_clickhouse(query=query_messages, connection = connection)

        return df_cube_message
    
    @task
        ## Объединяем таблицы
    def join_tables(df_cube_feed, df_cube_message):
        final_table = pd.merge(df_cube_feed, df_cube_message, on=['id','event_date'], \
                               how = 'outer')
        return final_table
    
    @task
    ## 3. Для этой таблицы считаем все эти метрики в разрезе по полу, юзеру и ос
    ## метрика с группировкой по os
    def metric_os(final_table):

            os = final_table.groupby('os').agg({'event_date':'min', \
                                    'likes':'sum', \
                                    'views': 'sum', \
                                    'messages_recieved':'sum', \
                                    'users_received':'sum', \
                                    'messages_sent':'sum', \
                                    'users_sent':'sum'}).reset_index().copy()
            os['metric'] = 'os'
            os.rename(columns={'os':'metric_value'},inplace=True)
            return os
        
    @task
    ## метрика с группировкой по полу
    
    def metric_gender(final_table):
        
        ## функция преобразования пола
        def transform_gender(x):
            if x == 1:
                return 'male'
            else:
                return 'female'

        gender = final_table.groupby('gender').agg({'event_date':'min', \
                                    'likes':'sum', \
                                    'views': 'sum', \
                                    'messages_recieved':'sum', \
                                    'users_received':'sum', \
                                    'messages_sent':'sum', \
                                    'users_sent':'sum'}).reset_index().copy()
        gender['metric'] = 'gender'
        gender.rename(columns={'gender':'metric_value'},inplace=True)
        gender['metric_value'] = gender['metric_value'].apply(transform_gender)
        return gender
    
    @task
    ## метрика с группировкой по возрасту

    def metric_age(final_table):
        
        # функция преобразования возраста
        def transform_age(x):
            if 0 <= x <= 16:
                return '0-16'
            elif 17 <= x <= 25:
                return '17-25'
            elif 26 <= x <= 45:
                return '26-45'
            elif 46 <= x <= 60:
                return '46-60'
            else:
                return '60+'

        final_table['age'] =final_table['age'].apply(transform_age)
        age = final_table.groupby('age').agg({'event_date':'min', \
                                    'likes':'sum', \
                                    'views': 'sum', \
                                    'messages_recieved':'sum', \
                                    'users_received':'sum', \
                                    'messages_sent':'sum', \
                                    'users_sent':'sum'}).reset_index().copy()
        age['metric'] = 'age'
        age.rename(columns={'age':'metric_value'},inplace=True)
        return age
    
    @task
    ## 4. И финальную данные со всеми метриками записываем в отдельную таблицу в ClickHouse
    ##объединение метрик в таблицу
    def join_metrics(os, gender, age):
        union_metrics = pd.concat([os, gender, age])

        new_cols = ['event_date',
                    'metric',
                    'metric_value',
                    'views',
                    'likes',
                    'messages_recieved',
                    'messages_sent',
                    'users_received',
                    'users_sent']  

        final_metrics = union_metrics.loc[:, new_cols]
        final_metrics = final_metrics.reset_index().drop('index', axis =1)
        final_metrics['event_date']=final_metrics['event_date'].apply(lambda x: datetime.isoformat(x))
        final_metrics = final_metrics.astype({
                       'views':'int', \
                       'likes':'int', \
                       'messages_recieved':'int', \
                       'messages_sent':'int', \
                       'users_received':'int', \
                       'users_sent':'int'})              
        return final_metrics
    
    @task
    ## запись в таблицу Clickhouse
    def load(final_metrics):
        ph.to_clickhouse(df=final_metrics, table='ovfedorova', index=False, \
                         connection = connection1)
        ## print(final_metrics.to_csv(index=False, sep='\t'))
        
    
    df_cube_feed = extract_feed()
    df_cube_message = extract_messages()
    
    final_table = join_tables(df_cube_feed, df_cube_message)
    
    os = metric_os(final_table)
    gender = metric_gender(final_table)
    age = metric_age(final_table)
    
    final_metrics = join_metrics(os, gender, age)
    
    load(final_metrics)
    
dag_ofedorova = dag_ofedorova()
    
    