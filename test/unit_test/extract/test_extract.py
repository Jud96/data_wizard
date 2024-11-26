import pandas as pd
import sqlite3
import unittest
from datetime import datetime
from unittest.mock import patch

from src.Extract.extract_service_from_csv import ExtractServiceFromCSV
from src.Extract.extract_service_from_api import ExtractServiceFromAPI
from src.Extract.extract_service_from_xml import ExtractServiceFromXML
from src.Extract.extract_service_from_sqlite import ExtractServiceFromsqlite


class TestExtract(unittest.TestCase):

    @classmethod
    def setup_sql_db(self):
        # source data store
        db_path = 'data/test_db.db'

        sql_script = 'data/source_data.sql'
        conn = sqlite3.connect(db_path)
        with open(sql_script, 'r') as f:
            conn.executescript(f.read())
        conn.close()
        return db_path

    def test_csv_extraction(self):
        extractor = ExtractServiceFromCSV('data/orders.csv')
        data = extractor.extract()
        print(data)
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 5)
        self.assertIn('source', data.columns)
        self.assertTrue(all(data['source'] == 'csv'))

    @patch('requests.get')
    def test_api_json_extraction(self, mock_get):
        api_url = 'https://api.example.com/data'
        mock_get.return_value.json.return_value = [
            {"identifier": 1, "date": "2024-07-01", "quantity": 10, "price": 9.99},
            {"identifier": 2, "date": "2024-07-02",
                "quantity": 15, "price": 19.99},
            {"identifier": 3, "date": "2024-07-03", "quantity": 7, "price": 14.99},
            {"identifier": 4, "date": "2024-07-04",
                "quantity": None, "price": 29.99},
            {"identifier": 5, "date": "2024-07-05", "quantity": 20, "price": 9.99}
        ]
        extractor = ExtractServiceFromAPI(api_url)
        data = extractor.extract()
        print(data)
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 5)
        self.assertIn('source', data.columns)
        self.assertTrue(all(data['source'] == 'api'))

    def test_xml_extraction(self):
        extractor = ExtractServiceFromXML('data/orders.xml')
        data = extractor.extract()
        print(data)
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 5)
        self.assertIn('source', data.columns)
        self.assertTrue(all(data['source'] == 'xml'))

    def test_sqlite_extraction(self):
        db_path = self.setup_sql_db()
        extractor = ExtractServiceFromsqlite(db_path, 'SELECT * FROM sales')
        data = extractor.extract()
        print(data)
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 5)
        self.assertIn('source', data.columns)
        self.assertTrue(all(data['source'] == 'sql'))


if __name__ == '__main__':
    unittest.main()
