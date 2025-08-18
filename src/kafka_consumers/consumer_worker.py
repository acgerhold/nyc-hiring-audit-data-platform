from src.utils.kafka import create_kafka_consumer, subscribe_to_topic
from src.utils.minio import create_minio_client
from src.kafka_consumers.ingestion_consumer import listen_for_ingestion
from src.utils.logger import setup_logger

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--bucket", required=True)
parser.add_argument("--topic", required=True)
parser.add_argument("--number", required=True)
args = parser.parse_args()

bucket = args.bucket
topic = args.topic
number = args.number

logger = setup_logger(f"ingestion_consumer_{number}", f"ingestion.log")

minio_client = create_minio_client()

consumer = create_kafka_consumer()
subscribe_to_topic(consumer, topic)
listen_for_ingestion(minio_client, bucket, consumer, logger)