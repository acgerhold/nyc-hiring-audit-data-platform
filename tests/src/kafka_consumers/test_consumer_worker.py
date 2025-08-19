
import sys
import pytest
from unittest.mock import patch, MagicMock
import src.kafka_consumers.consumer_worker as consumer_worker

class TestConsumerWorker:
	@patch('src.kafka_consumers.consumer_worker.listen_for_ingestion')
	@patch('src.kafka_consumers.consumer_worker.subscribe_to_topic')
	@patch('src.kafka_consumers.consumer_worker.create_kafka_consumer')
	@patch('src.kafka_consumers.consumer_worker.create_minio_client')
	@patch('src.kafka_consumers.consumer_worker.setup_logger')
	@patch('argparse.ArgumentParser.parse_args')
	def test_main_flow(self, mock_parse_args, mock_setup_logger, mock_create_minio_client, mock_create_kafka_consumer, mock_subscribe_to_topic, mock_listen_for_ingestion):
		# Arrange
		mock_args = MagicMock()
		mock_args.bucket = 'test-bucket'
		mock_args.topic = 'test-topic'
		mock_args.number = '1'
		mock_parse_args.return_value = mock_args

		mock_logger = MagicMock()
		mock_setup_logger.return_value = mock_logger
		mock_minio_client = MagicMock()
		mock_create_minio_client.return_value = mock_minio_client
		mock_consumer = MagicMock()
		mock_create_kafka_consumer.return_value = mock_consumer

		consumer_worker.main()

		# Assert
		mock_setup_logger.assert_called_once_with('ingestion_consumer_1', 'ingestion.log')
		mock_create_minio_client.assert_called_once()
		mock_create_kafka_consumer.assert_called_once()
		mock_subscribe_to_topic.assert_called_once_with(mock_consumer, 'test-topic')
		mock_listen_for_ingestion.assert_called_once_with(mock_minio_client, 'test-bucket', mock_consumer, mock_logger)

	def test_missing_args(self):
		test_argv = ["consumer_worker.py"]  # No args
		with patch.object(sys, 'argv', test_argv), \
			 patch('sys.exit', side_effect=SystemExit), \
			 patch('sys.stderr'):
			with pytest.raises(SystemExit):
				consumer_worker.main()
