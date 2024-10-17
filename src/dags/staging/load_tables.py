import pandas as pd
from io import StringIO
from helpers.my_minio import * 
from sqlalchemy import create_engine, Date
from time import sleep
from helpers.credentials import *

# support function
def remove_prefixes(input_string):
    # List of prefixes to remove
    prefixes = ["binance.", "gecko."]
    
    # Iterate through the prefixes and remove them from the string if present
    for prefix in prefixes:
        if input_string.startswith(prefix):
            input_string = input_string[len(prefix):]
    
    return input_string

def load_to_db(bucket_name): # bucket_name >>> table in database
    # minio connection
    client = connect_to_minio()
    # db connection
    db_name = "oltp"
    engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres/{db_name}')

    if bucket_name in ["spark-output"]:
        prefix_list = []
        object_list = []
        prefixes = client.list_objects(bucket_name)
        for pre in prefixes:
            prefix_list.append(pre.object_name.rstrip("/"))

        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            if obj.object_name.endswith(".csv"):
                object_list.append(obj.object_name)

        # Print out progress
        print(f"There are {len(prefix_list)} files to load to db_staging")
        print("The script will load spark-output to db staging")
        for index, pre in enumerate(prefix_list):
            print(f"- File {index+1}: {pre}")
        print()

        # read data from minio
        for pre in prefix_list:
            data = pd.DataFrame()
            # For each sub-bucket, loops through items
            # print(f"> Reading file {pre} ...")
            for obj in object_list:
                if pre in obj:
                    temp = client.get_object(bucket_name, obj).read().decode("utf-8")
                    df_temp = pd.read_csv(StringIO(temp), sep=",")
                    data = pd.concat([data, df_temp])
            
            # Modify the name of pre
            pre = remove_prefixes(pre)

            # Check exists date column in df, if exists, convert.
            if "date" in list(data.columns):
                data["date"] = pd.to_datetime(data["date"], format="%Y-%m-%d")
                data.to_sql(pre, engine, if_exists='replace', index=False, dtype={"date": Date()})
                # Print out progress
                print(f"-> Wrote {pre} to Db")
                print(data.head(3))
            else: 
                # Write to db
                data.to_sql(pre, engine, if_exists='replace', index=False)
                # Print out progress
                print(f"-> Wrote {pre} to Db")
                print(data.head(3))










