import json
import os

def listen_for_ingestion(minio_client, bucket_name, consumer, logger):
    logger.info("Listening for ingestion events...")
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode('utf-8'))
        file_path = event.get("path")
        filename = event.get("filename")
        if file_path and os.path.exists(file_path):
            minio_client.fput_object(bucket_name, filename, file_path)
            logger.info(f"Uploaded {filename} to MinIO bucket {bucket_name}")
        else:
            logger.info(f"File not found: {file_path}")