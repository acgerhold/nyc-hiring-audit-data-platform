from confluent_kafka import Producer
import json

def get_kafka_producer():
    return Producer({'bootstrap.servers': 'localhost:9092'})

def send_file_events(producer, topic, events):
    for event in events:
        producer.produce(topic, json.dumps(event).encode('utf-8'))
    producer.flush()