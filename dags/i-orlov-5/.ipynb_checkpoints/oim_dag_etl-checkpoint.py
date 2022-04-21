# coding=utf-8
import os

os.system("pip install pandahouse")

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import numpy as np
from io import StringIO

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

upload_connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}
            
# Default parameters for tasks
default_args = {
    'owner': 'i-orlov-5',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 4, 10),
}

# DAG run interval
schedule_interval = '0 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def oim_dag_etl():
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
    
    create_query = """
        CREATE TABLE IF NOT EXISTS test.oim
        (
            event_date Date,
            metric String,
            metric_value String,
            views Int32,
            likes Int32,
            messages_received Int32,
            messages_sent Int32,
            users_received Int32,
            users_sent Int32 
        ) ENGINE = MergeTree
        ORDER BY event_date
        """
    
    val_names = ('views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent')
    
    @task
    def create_table():
        ph.execute(connection=upload_connection, query=create_query)
    
    def extract_data(query):
        return Getch(query).df
    
    @task
    def extract_feed():
        return extract_data(feed_query)
    
    @task
    def extract_msg():
        return extract_data(msg_query)
    
    @task
    def join_dfs(df1, df2):
        return df1.merge(df2, how='outer', on=['id', 'event_date', 'gender', 'age', 'os', 'source'])
    
    def transform_metric(df, metric_name):
        res = (
            df[['event_date', metric_name, *val_names]]
            .groupby(['event_date', metric_name], as_index=False).sum()
            .rename(columns={metric_name: 'metric_value'})
        )
        
        res[[*val_names]] = res[[*val_names]].astype(int)
        res['metric_value'] = res['metric_value'].astype('U')
        res.insert(1, 'metric', metric_name)
        return res
    
    @task
    def transform_os(df):
        return transform_metric(df=df, metric_name='os')
    
    @task
    def transform_gender(df):
        return transform_metric(df=df, metric_name='gender')
    
    @task
    def transform_age(df, stratify):
        metric_name = 'age'
        # This approach was given up because quantiles are not consistent across days
        # quantiles = [df.age.quantile(q) for q in np.arange(0, 1.1, 0.2)]
        # quantiles = [quantiles[0]-1] + quantiles
        
        age_strats = ((0, 13), (14, 17), (18, 28), (29, 40), (41, 55), (56, 75), (76, 100))
                    
        res = (
            df[['event_date', metric_name, *val_names]]
            .groupby(['event_date', metric_name], as_index=False).sum()
            .rename(columns={metric_name: 'metric_value'})
        )
                
        res_strat = res
        
        if stratify:
            res_strat = pd.DataFrame(columns=list(res))
            for i,strat in enumerate(age_strats):
                res_strat = (
                    res_strat
                    .append(res[(res['metric_value'] >= strat[0]) 
                                & (res['metric_value'] <= strat[1])][[*val_names]]
                            .sum().astype(int), ignore_index=True)
                )
                res_strat.loc[i, 'metric_value'] = f"""{strat[0]:.0f}-{strat[1]:.0f}"""

        res_strat.loc[:, 'event_date'] = df.event_date[0]
        res_strat[[*val_names]] = res_strat[[*val_names]].astype(int)
        res_strat['metric_value'] = res_strat['metric_value'].astype('U')
        res_strat.insert(1, 'metric', metric_name)
        return res_strat
    
    @task
    def load(*args):
        df = pd.concat(args).reset_index().drop('index', axis=1)
        
        context = get_current_context()
        print(f"""Res {context['ds']}\nData to insert:""")
        print(df.to_csv(index=False, sep='\t'))
        
        ph.to_clickhouse(df, table='oim', connection=upload_connection, index=False)
        
        df_ch = Getch("select * from test.oim").df.sort_values(['event_date', 'metric'], ascending=[True, False])
        print("\nData selected:")
        print(df_ch.to_csv(index=False, sep='\t'))
        
        
    create_table()
    
    feed_df = extract_feed()
    msg_df = extract_msg()
    
    merged_df = join_dfs(feed_df, msg_df)
    
    os_df = transform_os(df=merged_df)
    gender_df = transform_gender(df=merged_df)
    age_df = transform_age(df=merged_df, stratify=True)
    
    load(os_df, gender_df, age_df)

oim_dag_etl = oim_dag_etl()

