import unittest
from unittest.mock import patch, MagicMock
from src.LinkedService.linked_service_to_mongodb import LinkedServiceToMongoDB  # Replace with the actual module name


class TestLinkedServiceToMongoDB(unittest.TestCase):
    def setUp(self):
        self.db_name = "test_db"
        self.user = "test_user"
        self.password = "test_password"
        self.host = "localhost"
        self.port = "27017"
        self.service = LinkedServiceToMongoDB(
            db_name=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    @patch("pymongo.MongoClient")
    def test_connect_success(self, mock_mongo_client):
        # Mock a successful MongoDB connection
        mock_db = MagicMock()
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_mongo_client.return_value = mock_client

        conn = self.service.connect()

        # Verify MongoClient was called with the correct arguments
        mock_mongo_client.assert_called_once_with(
            f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/"
        )
        # Ensure the returned connection is the mocked database
        self.assertEqual(conn, mock_db)

    @patch("pymongo.MongoClient")
    def test_connect_failure(self, mock_mongo_client):
        # Simulate a connection failure
        mock_mongo_client.side_effect = Exception("Connection failed")

        conn = self.service.connect()

        # Verify that None is returned in case of failure
        self.assertIsNone(conn)

    @patch("pymongo.MongoClient")
    def test_get_metadata_success(self, mock_mongo_client):
        # Mock a successful connection and metadata retrieval
        mock_db = MagicMock()
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_mongo_client.return_value = mock_client

        # Mock the list_collection_names and count_documents methods
        mock_db.list_collection_names.return_value = ["collection1", "collection2"]
        mock_db["collection1"].count_documents.return_value = 10
        mock_db["collection2"].count_documents.return_value = 20

        metadata = self.service.get_metadata()

        # Verify list_collection_names was called
        mock_db.list_collection_names.assert_called_once()
        # Verify count_documents was called for each collection
        mock_db["collection1"].count_documents.assert_called_once_with({})
        mock_db["collection2"].count_documents.assert_called_once_with({})

        # Check the metadata output
        expected_metadata = [
            {'collection': "collection1", 'count': 10},
            {'collection': "collection2", 'count': 20},
        ]
        self.assertEqual(metadata, expected_metadata)

    @patch("pymongo.MongoClient")
    def test_get_metadata_failure(self, mock_mongo_client):
        # Simulate a failure during metadata retrieval
        mock_mongo_client.side_effect = Exception("Metadata retrieval failed")

        metadata = self.service.get_metadata()

        # Ensure the method returns None in case of failure
        self.assertIsNone(metadata)


if __name__ == "__main__":
    unittest.main()

