import pytest
from unittest.mock import patch, MagicMock
from src.utils import kafka as kafka_utils

class TestKafkaUtils:
	@patch('src.utils.kafka.Producer')
	def test_create_kafka_producer(self, mock_producer):
		mock_instance = MagicMock()
		mock_producer.return_value = mock_instance
		producer = kafka_utils.create_kafka_producer()
		mock_producer.assert_called_once_with({'bootstrap.servers': 'localhost:9092'})
		assert producer == mock_instance

	@patch('src.utils.kafka.Consumer')
	def test_create_kafka_consumer(self, mock_consumer):
		mock_instance = MagicMock()
		mock_consumer.return_value = mock_instance
		consumer = kafka_utils.create_kafka_consumer()
		mock_consumer.assert_called_once()
		assert consumer == mock_instance

	@patch('src.utils.kafka.AdminClient')
	@patch('src.utils.kafka.NewTopic')
	def test_create_kafka_topic(self, mock_newtopic, mock_adminclient):
		mock_admin = MagicMock()
		mock_adminclient.return_value = mock_admin
		mock_future = MagicMock()
		mock_admin.create_topics.return_value = {'test-topic': mock_future}
		mock_future.result.return_value = None
		mock_topic = MagicMock()
		mock_newtopic.return_value = mock_topic

		kafka_utils.create_kafka_topic('test-topic', num_partitions=2, replication_factor=1)
		mock_adminclient.assert_called_once_with({'bootstrap.servers': 'localhost:9092'})
		mock_newtopic.assert_called_once_with(topic='test-topic', num_partitions=2, replication_factor=1)
		mock_admin.create_topics.assert_called_once_with([mock_topic])
		mock_future.result.assert_called_once()

	@patch('src.utils.kafka.logger')
	def test_produce_event(self, mock_logger):
		mock_producer = MagicMock()
		event = {'foo': 'bar'}
		kafka_utils.produce_event(mock_producer, 'topic', event)
		mock_producer.produce.assert_called_once()
		mock_producer.poll.assert_called_once_with(0)
		mock_logger.info.assert_called()

	def test_subscribe_to_topic(self):
		mock_consumer = MagicMock()
		kafka_utils.subscribe_to_topic(mock_consumer, 'topic')
		mock_consumer.subscribe.assert_called_once_with(['topic'])
