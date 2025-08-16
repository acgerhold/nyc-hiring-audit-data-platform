import os
import pytest
from unittest.mock import patch, MagicMock
from src.ingestion.data_acquisition import download_files
from src.ingestion.config import Config

@patch('src.ingestion.data_acquisition.requests.get')
@patch('src.ingestion.data_acquisition.tqdm')
def test_download_files_success(mock_tqdm, mock_get, tmp_path, monkeypatch):
	# Setup mocks
	mock_response = MagicMock()
	mock_response.iter_content = lambda chunk_size: [b'data']
	mock_response.headers = {'content-length': '4'}
	mock_response.raise_for_status = lambda: None
	mock_get.return_value = mock_response
	mock_tqdm.return_value.__enter__.return_value = MagicMock(update=lambda x: None)

	test_url = 'http://example.com/test.csv'
	test_filename = 'test.csv'
	# Patch Config.DATA_PATH to tmp_path for isolation
	monkeypatch.setattr(Config, 'DATA_PATH', str(tmp_path))

	events = download_files({test_url: test_filename})
	# Should be in tmp_path/other/test.csv due to match-case logic
	test_path = tmp_path / 'other' / test_filename
	assert test_path.exists()
	assert test_path.read_bytes() == b'data'
	assert events[0]['filename'] == test_filename
	assert events[0]['status'] == 'downloaded'

@patch('src.ingestion.data_acquisition.requests.get')
@patch('src.ingestion.data_acquisition.tqdm')
def test_download_files_error(mock_tqdm, mock_get, tmp_path, monkeypatch):
	# Setup mocks to raise exception
	mock_response = MagicMock()
	mock_response.raise_for_status.side_effect = Exception('Download failed')
	mock_get.return_value = mock_response
	mock_tqdm.return_value.__enter__.return_value = MagicMock(update=lambda x: None)

	test_url = 'http://example.com/test.csv'
	test_filename = 'test.csv'
	monkeypatch.setattr(Config, 'DATA_PATH', str(tmp_path))

	with pytest.raises(Exception, match='Download failed'):
		events = download_files({test_url: test_filename})

@patch('src.ingestion.data_acquisition.requests.get')
@patch('src.ingestion.data_acquisition.tqdm')
def test_download_files_event_structure(mock_tqdm, mock_get, tmp_path, monkeypatch):
	# Setup mocks
	mock_response = MagicMock()
	mock_response.iter_content = lambda chunk_size: [b'data']
	mock_response.headers = {'content-length': '4'}
	mock_response.raise_for_status = lambda: None
	mock_get.return_value = mock_response
	mock_tqdm.return_value.__enter__.return_value = MagicMock(update=lambda x: None)

	test_url = 'http://example.com/test.csv'
	test_filename = 'test.csv'
	monkeypatch.setattr(Config, 'DATA_PATH', str(tmp_path))

	events = download_files({test_url: test_filename})
	assert isinstance(events, list)
	assert len(events) == 1
	event = events[0]
	assert event['filename'] == test_filename
	assert event['path'].endswith(test_filename)
	assert event['status'] == 'downloaded'
	assert 'timestamp' in event
