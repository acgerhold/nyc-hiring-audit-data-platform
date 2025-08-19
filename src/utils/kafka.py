from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from src.utils.logger import setup_logger
import json

logger = setup_logger('kafka', 'kafka.log')

def create_kafka_producer():
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    logger.info(f"Created producer: {producer}")
    return producer

def create_kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'minio-uploader',
        'auto.offset.reset': 'earliest'
    })

    logger.info(f"Created consumer: {consumer}")
    return consumer

def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    new_topic = NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            logger.info(f"Topic: '{topic}' created successfully.")
        except Exception as e:
            if hasattr(e, 'args') and e.args and 'TOPIC_ALREADY_EXISTS' in str(e):
                logger.warning(f"Topic: '{topic}' already exists, proceeding...")
            else:
                logger.error(f"Failed to create topic: '{topic}', Error: {e}")

def produce_event(producer, topic_name, event):
    producer.produce(topic_name, json.dumps(event).encode('utf-8'))
    producer.poll(0)

    logger.info(f"Produced: {event} to topic: {topic_name}")

def subscribe_to_topic(consumer, topic_name):
    consumer.subscribe([f'{topic_name}'])

    logger.info(f"Consumer: {consumer} subscribed to topic: {topic_name}")