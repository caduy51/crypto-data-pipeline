# Airflow
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# Storage
from io import StringIO
import pandas as pd
from sqlalchemy import create_engine
# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
# Utilities
import json, requests, pendulum, os, datetime, time, csv, re, sys
# Refence Gecko
from gecko.get_tasks import  *
from gecko.load_tasks import *
# Refence Binance
from binance.get_tasks import *
from binance.load_tasks import *
# Reference postgre
from staging.load_tables import *
# Helpers
from helpers.my_minio import connect_to_minio, load_to_minio

@dag(dag_id="crypto", start_date=datetime.datetime(2024, 8, 15), schedule_interval="@daily", catchup=False)
def etl():
    # Binance
    get_symbols_task = PythonOperator(task_id = "get_symbols", python_callable=get_symbols)
    get_bars_task = PythonOperator(task_id = "get_bars", python_callable=get_bars)
    
    load_symbols_task = PythonOperator(task_id = "load_symbols", python_callable=load_symbols)
    load_bars_task = PythonOperator(task_id = "load_bars", python_callable=load_bars)

    # Gecko
    get_category_task = PythonOperator(task_id = "get_category", python_callable = get_category)
    get_category_details_task = PythonOperator(task_id = "get_category_details", python_callable = get_category_details)
    load_category_task = PythonOperator(task_id="load_category", python_callable=load_category)
    load_category_details_task = PythonOperator(task_id="load_category_details", python_callable=load_category_details)
    # Wait for all loads done
    all_loads_done = EmptyOperator(task_id="all_loads_done")
    # Spark process
    spark_process = SparkSubmitOperator(
        task_id = "spark_process",
        application="/opt/airflow/dags/spark_app/spark.py", # inside airflow container
        packages='org.apache.hadoop:hadoop-aws:3.3.1', 
        conn_id="spark-default"
    )
    
    # Write Spark Output to Minio
    load_to_postgres_task = PythonOperator(
                                task_id = "load_to_postgres", 
                                python_callable = load_to_db, 
                                op_kwargs = {'bucket_name': "spark-output"}
    )

    # Data pipelines
    # Binance
    get_symbols_task >> get_bars_task >> [load_symbols_task, load_bars_task] >> all_loads_done
    # Gecko
    get_category_task >> get_category_details_task >> [load_category_task, load_category_details_task] >> all_loads_done
    all_loads_done >> spark_process >> load_to_postgres_task
etl()

