from unittest.mock import MagicMock, patch, call
import json
from src.kafka_consumers.ingestion_consumer import listen_for_ingestion

class TestIngestionConsumer:
    @staticmethod
    def make_msg(value=None, error=None):
        m = MagicMock()
        m.value.return_value = value
        m.error.return_value = error
        return m

    # Mocks a valid event and checks that MinIO’s fput_object is called and the logger logs the upload.
    @patch('os.path.exists')
    def test_valid_event_uploads_to_minio(self, mock_exists):
        minio_client = MagicMock()
        consumer = MagicMock()
        logger = MagicMock()
        event = {"filename": "file.csv", "path": "/tmp/file.csv"}
        consumer.poll.side_effect = [self.make_msg(json.dumps(event).encode('utf-8'), None), KeyboardInterrupt]
        mock_exists.return_value = True
        try:
            listen_for_ingestion(minio_client, "bucket", consumer, logger)
        except KeyboardInterrupt:
            pass
        minio_client.fput_object.assert_called_once_with("bucket", "file.csv", "/tmp/file.csv")
        logger.info.assert_any_call("Uploaded file.csv to MinIO bucket bucket")

    # Mocks a Kafka message with an error and checks that the logger logs the error.
    def test_consumer_error_logs_error(self):
        minio_client = MagicMock()
        consumer = MagicMock()
        logger = MagicMock()
        consumer.poll.side_effect = [self.make_msg(None, "Some error"), KeyboardInterrupt]
        try:
            listen_for_ingestion(minio_client, "bucket", consumer, logger)
        except KeyboardInterrupt:
            pass
        logger.error.assert_any_call("Consumer error: Some error")

    # Mocks a valid event but with a missing file, and checks that the logger logs “File not found.”
    @patch('os.path.exists')
    def test_file_not_found_logs_info(self, mock_exists):
        minio_client = MagicMock()
        consumer = MagicMock()
        logger = MagicMock()
        event = {"filename": "file.csv", "path": "/tmp/file.csv"}
        consumer.poll.side_effect = [self.make_msg(json.dumps(event).encode('utf-8'), None), KeyboardInterrupt]
        mock_exists.return_value = False
        try:
            listen_for_ingestion(minio_client, "bucket", consumer, logger)
        except KeyboardInterrupt:
            pass

        logger.info.assert_any_call("File not found: /tmp/file.csv")

    # Mocks a 'None' message (no event) and ensure the logger doesn't log an error
    def test_none_message_continues(self):
        minio_client = MagicMock()
        consumer = MagicMock()
        logger = MagicMock()
        # None message, then KeyboardInterrupt to break loop
        consumer.poll.side_effect = [None, KeyboardInterrupt]

        try:
            listen_for_ingestion(minio_client, "bucket", consumer, logger)
        except KeyboardInterrupt:
            pass

        # Should not call error/info for None
        logger.error.assert_not_called()
