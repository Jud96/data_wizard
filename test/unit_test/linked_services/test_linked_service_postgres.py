import unittest
from unittest.mock import patch, MagicMock
from src.LinkedService.linked_service_to_db import LinkedServiceToDB
from src.LinkedService.linked_service_postgres import LinkedServicePostgres

class TestLinkedServicePostgres(unittest.TestCase):
    def setUp(self):
        self.db_name = "test_db"
        self.user = "test_user"
        self.password = "test_password"
        self.host = "localhost"
        self.port = "5432"
        self.service = LinkedServicePostgres(
            db_name=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    @patch("psycopg2.connect")
    def test_connect_success(self, mock_connect):
        # Mock a successful connection
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        conn = self.service.connect()

        # Verify that psycopg2.connect was called with the correct parameters
        mock_connect.assert_called_once_with(
            database=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        # Assert that the returned connection is the mocked connection
        self.assertEqual(conn, mock_connection)

    @patch("psycopg2.connect")
    def test_connect_failure(self, mock_connect):
        # Mock a connection failure
        mock_connect.side_effect = Exception("Connection failed")

        with self.assertRaises(Exception) as context:
            self.service.connect()

        self.assertEqual(str(context.exception), "Connection failed")

    @patch("psycopg2.connect")
    def test_get_metadata_success(self, mock_connect):
        # Mock the connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock the cursor's behavior
        mock_cursor.fetchall.return_value = [
            ("public", "table1", "column1", "integer"),
            ("public", "table1", "column2", "text"),
        ]

        # Call the method
        metadata = self.service.get_metadata()

        # Verify the SQL execution
        mock_cursor.execute.assert_called_once_with("""
            SELECT table_schema, table_name, column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name, ordinal_position;
        """)

        # Verify that the cursor and connection were closed
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

        # Verify the metadata returned
        expected_metadata = [
            ("public", "table1", "column1", "integer"),
            ("public", "table1", "column2", "text"),
        ]
        self.assertEqual(metadata, expected_metadata)

    @patch("psycopg2.connect")
    def test_get_metadata_failure(self, mock_connect):
        # Mock a connection failure
        mock_connect.side_effect = Exception("Connection failed")

        with self.assertRaises(Exception) as context:
            self.service.get_metadata()

        self.assertEqual(str(context.exception), "Connection failed")

if __name__ == "__main__":
    unittest.main()
