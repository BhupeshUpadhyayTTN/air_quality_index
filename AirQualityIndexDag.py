from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime,timedelta
import sys
sys.path.append('/home/bhupesh/Desktop/air_quality_index/')
from ParseData import fetchData
from Variables import *
from SparkAggregation import sparkCode
from AthenaQuery import athena_query

default_args = {
    'owner':'bhupesh',
}

with DAG(
    'air_quality_index_dag',
    default_args=default_args,
    start_date=datetime(2023,11,14),
    schedule_interval=None,
    catchup=False
) as dag:
    
    start = DummyOperator(task_id='start')

    task1 = PythonOperator(task_id='data_fetching',
                           python_callable=fetchData)
    task2 = PythonOperator(task_id='spark_aggregation',
                           python_callable=sparkCode)
    task3 = PythonOperator(task_id='athena_query',
                           python_callable=athena_query)
    end = DummyOperator(task_id='end')

start >> task1 >> task2 >> task3 >> end