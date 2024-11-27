import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from psycopg2 import sql
from src.Datasets.dataset_from_db import DatasetFromDB

class TestDatasetFromDBDeltaLoad(unittest.TestCase):
    def setUp(self):
        # Mock connection and cursor
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.dataset = DatasetFromDB(conn=self.mock_conn, table_name="test_table")

    def test_delta_load_success(self):
        # Mock data
        data = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [25, 30]
        })
        key_columns = ["id"]

        self.dataset.delta_load(data, key_columns)

        # Assert cursor.execute is called correctly
        self.assertEqual(self.mock_cursor.execute.call_count, 2)  # Called once for each row
        self.mock_conn.commit.assert_called_once()

    def test_delta_load_rollback_on_error(self):
        # Mock data
        data = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [25, 30]
        })
        key_columns = ["id"]

        # Simulate an exception during execute
        self.mock_cursor.execute.side_effect = Exception("Test error")

        with self.assertRaises(Exception):
            self.dataset.delta_load(data, key_columns)

        self.mock_conn.rollback.assert_called_once()

if __name__ == "__main__":
    unittest.main()
