from unittest.mock import patch, MagicMock
from src.ingestion.data_acquisition import download_file
from src.ingestion.config import Config

class TestDataAcquisition:
	@patch('src.ingestion.data_acquisition.requests.get')
	@patch('src.ingestion.data_acquisition.tqdm')
	def test_download_file_success(self, mock_tqdm, mock_get, tmp_path, monkeypatch):
		mock_response = MagicMock()
		mock_response.iter_content = lambda chunk_size: [b'data']
		mock_response.headers = {'content-length': '4'}
		mock_response.raise_for_status = lambda: None
		mock_get.return_value = mock_response
		mock_tqdm.return_value.__enter__.return_value = MagicMock(update=lambda x: None)

		test_url = 'http://example.com/test.csv'
		test_filename = 'test.csv'
		monkeypatch.setattr(Config, 'DATA_PATH', str(tmp_path))

		event = download_file(test_url, test_filename)
		test_path = tmp_path / 'other' / test_filename
		assert test_path.exists()
		assert test_path.read_bytes() == b'data'
		assert event['filename'] == test_filename
		assert event['status'] == 'downloaded'

	@patch('src.ingestion.data_acquisition.requests.get')
	@patch('src.ingestion.data_acquisition.tqdm')
	def test_download_file_error(self, mock_tqdm, mock_get, tmp_path, monkeypatch):
		mock_response = MagicMock()
		mock_response.raise_for_status.side_effect = Exception('Download failed')
		mock_get.return_value = mock_response
		mock_tqdm.return_value.__enter__.return_value = MagicMock(update=lambda x: None)

		test_url = 'http://example.com/test.csv'
		test_filename = 'test.csv'
		monkeypatch.setattr(Config, 'DATA_PATH', str(tmp_path))

		event = download_file(test_url, test_filename)
		assert event['filename'] == test_filename
		assert event['status'] == 'failed'
		assert 'exception' in event

	@patch('src.ingestion.data_acquisition.requests.get')
	@patch('src.ingestion.data_acquisition.tqdm')
	def test_download_file_event_structure(self, mock_tqdm, mock_get, tmp_path, monkeypatch):
		mock_response = MagicMock()
		mock_response.iter_content = lambda chunk_size: [b'data']
		mock_response.headers = {'content-length': '4'}
		mock_response.raise_for_status = lambda: None
		mock_get.return_value = mock_response
		mock_tqdm.return_value.__enter__.return_value = MagicMock(update=lambda x: None)

		test_url = 'http://example.com/test.csv'
		test_filename = 'test.csv'
		monkeypatch.setattr(Config, 'DATA_PATH', str(tmp_path))

		event = download_file(test_url, test_filename)
		assert isinstance(event, dict)
		assert event['filename'] == test_filename
		assert event['path'].endswith(test_filename)
		assert event['status'] == 'downloaded'
		assert 'timestamp' in event
