import json
from unittest.mock import MagicMock
from src.kafka_producers.file_event_producer import send_file_events

def test_send_file_events_calls_produce_and_flush():
	mock_producer = MagicMock()
	topic = "test_topic"
	events = [
		{"filename": "file1.csv", "status": "downloaded"},
		{"filename": "file2.csv", "status": "failed"}
	]

	send_file_events(mock_producer, topic, events)

	# Check produce called for each event
	assert mock_producer.produce.call_count == len(events)
	for call, event in zip(mock_producer.produce.call_args_list, events):
		args, kwargs = call
		assert args[0] == topic
		# The message should be the JSON-encoded event
		assert json.loads(args[1].decode('utf-8')) == event

	# Check flush called once
	mock_producer.flush.assert_called_once()
