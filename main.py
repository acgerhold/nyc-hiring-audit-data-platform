from src.ingestion.config import Config
from src.ingestion.data_acquisition import download_file
from src.utils.kafka import create_kafka_producer, create_kafka_topic, produce_event
from src.utils.minio import create_minio_client, create_minio_bucket

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

url_to_name = {
    Config.NYC_OPENDATA_PAYROLL_DATA_DICT: "payroll_data_dict.xlsx",
    #Config.NYC_OPENDATA_PAYROLL_DATA: "payroll_data.csv",
    Config.NYC_OPENDATA_JOB_POSTINGS_DATA_DICT: "job_postings_data_dict.xlsx",
    Config.NYC_OPENDATA_JOB_POSTINGS_DATA: "job_postings_data.csv"
}

bucket = "nyc-data"
minio_client = create_minio_client()
create_minio_bucket(minio_client, bucket)

topic = "ingestion"
num_partitions = 3
create_kafka_topic(topic, num_partitions)

# How many consumers to dedicate to this topic
num_consumers = 3
for i in range(num_consumers):
    subprocess.Popen(
        [sys.executable, "-m", "src.kafka_consumers.consumer_worker", "--bucket", bucket, "--topic", topic, "--number", str(i + 1)]
    )

producer = create_kafka_producer()

# Downloading files in parallel
with ThreadPoolExecutor() as executor:
    future_to_file = {
        executor.submit(download_file, url, filename): filename
        for url, filename in url_to_name.items()
    }
    for future in as_completed(future_to_file):
        event = future.result()
        produce_event(producer, topic, event)




# docker compose exec kafka bash
# - Opens a shell inside the kafka container
# kafka-topics --bootstrap-server localhost:9092 --list
# - Lists all topics
# kafka-console-consumer --bootstrap-server localhost:9092 --topic topic_name --from-beginning
# - Checks what events/messages have been produced to a topic
# kafka-topics --bootstrap-server localhost:9092 --delete --topic topic_name
# - deletes a topic

# ps aux | grep consumer_worker
# - To list all consumer processes running in background
# pkill -f src.kafka_consumers.consumer_worker
# - To kill all consumer processes running in background
# - subprocess.run(["pkill", "-f", "src.kafka_consumers.consumer_worker"]) to end programmatically