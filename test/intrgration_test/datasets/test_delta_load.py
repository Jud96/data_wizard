import unittest
import psycopg2
import pandas as pd
from src.Datasets.dataset_from_db import DatasetFromDB  # Replace with your module name

class TestDatasetFromDBDeltaLoadIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Connect to a test database
        cls.conn = psycopg2.connect(database="test_db", user="user", password="password", host="localhost", port="5432")
        cls.dataset = DatasetFromDB(conn=cls.conn, table_name="test_table")
        cls.cursor = cls.conn.cursor()

        # Create a test table
        cls.cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT,
                age INT
            );
        """)
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        # Drop the test table and close the connection
        cls.cursor.execute("DROP TABLE IF EXISTS test_table;")
        cls.conn.commit()
        cls.cursor.close()
        cls.conn.close()

    def setUp(self):
        # Clear the table before each test
        self.cursor.execute("TRUNCATE TABLE test_table;")
        self.conn.commit()

    def test_delta_load_inserts_and_updates(self):
        # Initial data
        data = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [25, 30]
        })
        key_columns = ["id"]

        # Call delta_load to insert data
        self.dataset.delta_load(data, key_columns)

        # Verify data is inserted
        self.cursor.execute("SELECT * FROM test_table ORDER BY id;")
        results = self.cursor.fetchall()
        expected = [(1, "Alice", 25), (2, "Bob", 30)]
        self.assertEqual(results, expected)

        # Update data
        updated_data = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice Updated", "Bob Updated"],
            "age": [26, 31]
        })
        self.dataset.delta_load(updated_data, key_columns)

        # Verify data is updated
        self.cursor.execute("SELECT * FROM test_table ORDER BY id;")
        updated_results = self.cursor.fetchall()
        updated_expected = [(1, "Alice Updated", 26), (2, "Bob Updated", 31)]
        self.assertEqual(updated_results, updated_expected)

if __name__ == "__main__":
    unittest.main()
