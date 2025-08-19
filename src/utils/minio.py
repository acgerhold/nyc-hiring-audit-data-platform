from minio import Minio
from dotenv import load_dotenv
from src.utils.logger import setup_logger
import os

logger = setup_logger('minio', 'minio.log')

def create_minio_client():
    load_dotenv()
    minio_client = Minio(
        os.getenv('MINIO_EXTERNAL_URL'),
        access_key = os.getenv('MINIO_ACCESS_KEY'),
        secret_key = os.getenv('MINIO_SECRET_KEY'),
        secure = False
    )

    logger.info(f"Created MinIO Client: {minio_client}")
    return minio_client

def create_minio_bucket(minio_client, bucket_name):
    bucket_name = "nyc-data"
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    logger.info(f"Created MinIO bucket: {bucket_name}")