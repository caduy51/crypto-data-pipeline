# Minio
from minio import Minio
from io import BytesIO
from minio.error import S3Error
from helpers import credentials

def connect_to_minio():
    # Minio connection
    client = Minio(
        endpoint='minio:9000',  # Use the HTTP port
        access_key=credentials.access_key,
        secret_key=credentials.secret_key,
        secure=False  # Ensure this is False for HTTP
    )
    return client

def load_to_minio(file_path, bucket_name, object_name):
    client = connect_to_minio()
    # Check bucket exists?
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Upload to minio
    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=file_path
    )
    print(f"Successfully uploaded {object_name} to bucket {bucket_name}")




    