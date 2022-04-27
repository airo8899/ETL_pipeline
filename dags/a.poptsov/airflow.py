from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


connection_write = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }

def ch_get_df(query='Select *', connection = connection_write):
    r = requests.post(host, data=query.encode("utf-8"), auth=(connection['user'], connection['password']), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


connection_read = {'host': 'http://clickhouse.beslan.pro:8080',
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
schedule_interval = '0 5 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_poptsov():

    @task()
    def extract():
        query = """SELECT 
               toDate(time) as event_date, 
               country, 
               source,
               count() as likes
            FROM 
                simulator_20220320.feed_actions
            where 
                toDate(time) = '2022-04-21' 
                and action = 'like'
            group by
                event_date,
                country,
                source"""       
        
        df_cube = ch_get_df2(query=query, connection = connection_read)
        
        return df_cube

dag_sim_example = dag_poptsov()