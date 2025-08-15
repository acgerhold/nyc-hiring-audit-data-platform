
import os
import pytest
from unittest.mock import patch, MagicMock
from src.ingestion.data_acquisition import download_files

@patch('src.ingestion.data_acquisition.requests.get')
@patch('src.ingestion.data_acquisition.tqdm')
def test_download_files_success(mock_tqdm, mock_get, tmp_path):
	# Setup mocks
	mock_response = MagicMock()
	mock_response.iter_content = lambda chunk_size: [b'data']
	mock_response.headers = {'content-length': '4'}
	mock_response.raise_for_status = lambda: None
	mock_get.return_value = mock_response
	mock_tqdm.return_value.__enter__.return_value = MagicMock(update=lambda x: None)

	test_url = 'http://example.com/test.csv'
	test_filename = 'test.csv'
	test_path = tmp_path / test_filename
	# Patch os.path.join to use tmp_path
	with patch('os.path.join', side_effect=lambda *args: str(tmp_path / args[-1])):
		download_files({test_url: test_filename})
		assert test_path.exists()
		assert test_path.read_bytes() == b'data'

@patch('src.ingestion.data_acquisition.requests.get')
@patch('src.ingestion.data_acquisition.tqdm')
def test_download_files_error(mock_tqdm, mock_get, tmp_path):
	# Setup mocks to raise exception
	mock_response = MagicMock()
	mock_response.raise_for_status.side_effect = Exception('Download failed')
	mock_get.return_value = mock_response
	mock_tqdm.return_value.__enter__.return_value = MagicMock(update=lambda x: None)

	test_url = 'http://example.com/test.csv'
	test_filename = 'test.csv'
	with patch('os.path.join', side_effect=lambda *args: str(tmp_path / args[-1])):
		with pytest.raises(Exception, match='Download failed'):
			download_files({test_url: test_filename})
