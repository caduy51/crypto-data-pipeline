from helpers.my_minio import *

# Load Gecko
def load_category():
    minio_client = connect_to_minio()
    file_path = "/opt/airflow/data/gecko/category.json"
    bucket_name = "gecko-category"
    object_name = "category.json"
    load_to_minio(file_path, bucket_name, object_name)

def load_category_details():
    minio_client = connect_to_minio()
    file_path = "/opt/airflow/data/gecko/category_details.json"
    bucket_name = "gecko-category-details"
    object_name = "category-details.json"
    load_to_minio(file_path, bucket_name, object_name)