import os
os.system('pip install pandahouse')
os.system('pip install clickhouse-driver')
import pandahouse
from clickhouse_driver import Client
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests

#class for reading tables from CH
class Getch:
    def __init__(self, query, db='simulator'):
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
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)


def dag_bogoliubova():

    def extract_feed():
        
        #для каждого юзера посчитаем число просмотров и лайков контента
        
        df_feed = Getch("""SELECT 
                       toDate(time) as event_date,
                       user_id,
                       gender,
                       multiIf(age < 18, '0 - 18', age >= 18 and age < 24, '19-24',
                       age >= 24 and age < 34, '25-34', age >= 34 and age < 44, '35-44',
                       age >= 44 and age < 54, '45-54', '55+') as age,
                       os,
                       countIf(action = 'like') as likes,
                       countIf(action = 'view') as views
                    FROM 
                        simulator_20220320.feed_actions 
                    where 
                        toDate(time) = today() - 1
                    group by
                        event_date, user_id, gender, age, os""").df
        return df_feed
    
    def extract_mes():
        
        #для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему
        
        df_message = Getch("""
                    SELECT 
                       user_id,
                       count(reciever_id) as messages_sent,
                       uniq(reciever_id) as users_sent
                    FROM 
                        simulator_20220320.message_actions 
                    where 
                        toDate(time) = today() - 1
                    group by
                        user_id, reciever_id, gender, age, os""").df
                        
        df_message1 = Getch("""SELECT
                       reciever_id as user_id,
                       count(user_id) as messages_received,
                       uniq(user_id) as users_received
                     FROM
                        simulator_20220320.message_actions
                     where
                        toDate(time) = today() - 1
                     group by user_id""").df

        df_message = df_message.merge(df_message1, on='user_id', how='left')
        return df_message

    def merging(df_feed, df_message):
        return df_feed.merge(df_message, on='user_id', how='inner')
        
        
    def transform(df_cube):
        df_final = df_cube[['event_date', 'gender', 'age', 'os', 'views', 'likes', 
                          'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
            .groupby(['event_date', 'gender', 'age', 'os'], as_index=Flase)\
            .sum()
        df_final[['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']] = \
            df_final[['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']].astype(int)
        return df_final


    def load(df_final):
        connection = {
                        'host': 'https://clickhouse.lab.karpov.courses',
                        'password': '656e2b0c9c',
                        'user': 'student-rw',
                        'database': 'test'
        }
        if pandahouse.read_clickhouse('EXISTS TABLE test.bogoliubova_test', connection=connection)['result'][0] == 0:
            client = Client('clickhouse.lab.karpov.courses',
                password='656e2b0c9c',
                user='student-rw',
                database='test')
            q = '''CREATE TABLE IF NOT EXISTS test.bogoliubova_test
                (       
                    event_date Date,
                    gender UInt64,
                    age String,
                    os String,
                    views UInt64,
                    likes UInt64,
                    messages_received UInt64,
                    messages_sent UInt64,
                    users_received UInt64,
                    users_sent UInt64 
                ) ENGINE = Log()
                '''
            client.execute(q)

        if pandahouse.read_clickhouse("SELECT * FROM test.bogoliubova_test WHERE event_date = today()-1 LIMIT 10", connection=connection)['event_date'].count() == 0:
            pandahouse.to_clickhouse(df=df_final, table='bogoliubova_test', index=False, connection=connection)


    df_feed = extract_feed()
    df_message = extract_mes()
    df_cube = merging(df_feed, df_message)
    df_final = transform(df_cube)
    load(df_final)

dag_bogoliubova()
