import os
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from helpers.my_minio import * 

# Binance API
def load_bars():
    minio_client = connect_to_minio()
    get_bars_path = "/opt/airflow/data/get_bars"
    file_list = os.listdir(get_bars_path)
    bucket_name = "binance-bars"
    # Loops through files in get_bars_dir, load to minio
    for file_name in file_list:
        load_to_minio(os.path.join(get_bars_path, file_name), bucket_name, file_name)

def load_symbols():
    minio_client = connect_to_minio()
    file_path = "/opt/airflow/data/get_symbols/symbols.json"
    bucket_name = "binance-symbols"
    object_name = "symbols.json"
    load_to_minio(file_path, bucket_name, object_name)

def load_latest_prices():
    minio_client = connect_to_minio()
    # airflow local files
    latest_price_dir = "/opt/airflow/data/get_latest_prices"
    file_list = os.listdir(latest_price_dir)
    # minio info
    bucket_name = "binance-latest-prices"
    # Loops through airflow local files, upload to Minio
    for file_name in file_list:
        load_to_minio(os.path.join(latest_price_dir, file_name), bucket_name, file_name)

