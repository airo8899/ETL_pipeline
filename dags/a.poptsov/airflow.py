import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator # Так как мы пишет такси в питоне
from datetime import datetime 

query = 
"""
select *
"""