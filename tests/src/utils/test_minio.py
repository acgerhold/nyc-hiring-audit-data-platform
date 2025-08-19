import os
from unittest.mock import patch, MagicMock
from src.utils import minio as minio_utils

class TestMinioUtils:
	@patch('src.utils.minio.Minio')
	@patch('src.utils.minio.load_dotenv')
	@patch.dict(os.environ, {
		'MINIO_EXTERNAL_URL': 'localhost:9000',
		'MINIO_ACCESS_KEY': 'minio-access',
		'MINIO_SECRET_KEY': 'minio-secret'
	}, clear=True)
	
    # Test that create_minio_client loads env and instantiates Minio with correct args.
	def test_create_minio_client(self, mock_load_dotenv, mock_minio):
		mock_client = MagicMock()
		mock_minio.return_value = mock_client
		client = minio_utils.create_minio_client()
		mock_load_dotenv.assert_called_once()
		mock_minio.assert_called_once_with(
			'localhost:9000',
			access_key='minio-access',
			secret_key='minio-secret',
			secure=False
		)
		assert client == mock_client

    # Test that create_minio_bucket creates the bucket if it does not exist.
	@patch('src.utils.minio.Minio')
	def test_create_minio_bucket_creates_if_not_exists(self, mock_minio):
		mock_client = MagicMock()
		mock_client.bucket_exists.return_value = False
		minio_utils.create_minio_bucket(mock_client, 'nyc-data')
		mock_client.bucket_exists.assert_called_once_with('nyc-data')
		mock_client.make_bucket.assert_called_once_with('nyc-data')

    # Test that create_minio_bucket does not create the bucket if it already exists.
	@patch('src.utils.minio.Minio')
	def test_create_minio_bucket_does_not_create_if_exists(self, mock_minio):
		mock_client = MagicMock()
		mock_client.bucket_exists.return_value = True
		minio_utils.create_minio_bucket(mock_client, 'nyc-data')
		mock_client.bucket_exists.assert_called_once_with('nyc-data')
		mock_client.make_bucket.assert_not_called()
