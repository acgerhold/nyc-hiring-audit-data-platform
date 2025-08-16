from src.ingestion.data_acquisition import download_files
from src.kafka_producers.file_event_producer import get_kafka_producer, send_file_events
from src.ingestion.config import Config

url_to_name = {
    Config.NYC_OPENDATA_PAYROLL_DATA_DICT: "payroll_data_dict.xlsx",
    Config.NYC_OPENDATA_PAYROLL_DATA: "payroll_data.csv",
    Config.NYC_OPENDATA_JOB_POSTINGS_DATA_DICT: "job_postings_data_dict.xlsx",
    Config.NYC_OPENDATA_JOB_POSTINGS_DATA: "job_postings_data.csv"
}

producer = get_kafka_producer()
events = download_files(url_to_name)
send_file_events(producer, "data_ingestion", events)

# docker compose exec kafka bash
# - Opens a shell inside the kafka container
# kafka-console-consumer --bootstrap-server localhost:9092 --topic data_ingestion --from-beginning
# - Checks what events/messages have been pushed to a topic
# kafka-topics --bootstrap-server localhost:9092 --delete --topic data_ingestion
# - To delete a topic