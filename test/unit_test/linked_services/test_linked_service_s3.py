import unittest
from unittest.mock import patch, MagicMock
from src.LinkedService.linked_service_s3 import LinkedServiceS3  

class TestLinkedServiceS3(unittest.TestCase):
    def setUp(self):
        self.bucket_name = "test-bucket"
        self.aws_access_key_id = "test-access-key"
        self.aws_secret_access_key = "test-secret-key"
        self.region_name = "us-east-1"
        self.service = LinkedServiceS3(
            bucket_name=self.bucket_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

    @patch("boto3.client")
    def test_connect_success(self, mock_boto_client):
        # Mock successful connection to S3
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        s3 = self.service.connect()

        # Verify boto3 client creation
        mock_boto_client.assert_called_once_with(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            # region_name=self.region_name (commented out in class)
        )
        # Verify returned client
        self.assertEqual(s3, mock_s3_client)

    @patch("boto3.client")
    def test_connect_failure(self, mock_boto_client):
        # Mock a connection failure
        mock_boto_client.side_effect = Exception("Connection failed")

        with self.assertRaises(Exception) as context:
            self.service.connect()

        self.assertEqual(str(context.exception), "Connection failed")

    @patch("boto3.client")
    def test_get_metadata_success(self, mock_boto_client):
        # Mock the S3 client and its behavior
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {
                    'Key': 'file1.txt',
                    'LastModified': '2024-11-25T12:00:00.000Z',
                    'Size': 1024,
                    'StorageClass': 'STANDARD'
                },
                {
                    'Key': 'file2.jpg',
                    'LastModified': '2024-11-26T15:30:00.000Z',
                    'Size': 2048,
                    'StorageClass': 'STANDARD'
                },
            ]
        }

        metadata = self.service.get_metadata()

        # Verify the S3 list_objects_v2 call
        mock_s3_client.list_objects_v2.assert_called_once_with(Bucket=self.bucket_name)

        # Verify the returned metadata
        expected_metadata = [
            {
                'Key': 'file1.txt',
                'LastModified': '2024-11-25T12:00:00.000Z',
                'Size': 1024,
                'StorageClass': 'STANDARD'
            },
            {
                'Key': 'file2.jpg',
                'LastModified': '2024-11-26T15:30:00.000Z',
                'Size': 2048,
                'StorageClass': 'STANDARD'
            },
        ]
        self.assertEqual(metadata, expected_metadata)

    @patch("boto3.client")
    def test_get_metadata_no_contents(self, mock_boto_client):
        # Mock the S3 client and its behavior
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.return_value = {}

        metadata = self.service.get_metadata()

        # Verify the S3 list_objects_v2 call
        mock_s3_client.list_objects_v2.assert_called_once_with(Bucket=self.bucket_name)

        # Verify that metadata is empty when there are no objects
        self.assertEqual(metadata, [])

    @patch("boto3.client")
    def test_get_metadata_failure(self, mock_boto_client):
        # Mock a failure during metadata retrieval
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.side_effect = Exception("S3 error")

        with self.assertRaises(Exception) as context:
            self.service.get_metadata()

        self.assertEqual(str(context.exception), "S3 error")

if __name__ == "__main__":
    unittest.main()
