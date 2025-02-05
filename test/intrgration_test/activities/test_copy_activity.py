import unittest
import boto3

from src.LinkedService.linked_service_postgres import LinkedServicePostgres
from src.Activity.execute_sql import ExecuteSQL
from src.Datasets.dataset_from_datalake import DatasetFromDataLake
from src.Datasets.dataset_from_db import DatasetFromDB
from src.Datasets.dataset_from_api import DatasetFromAPI
from src.Activity.copy_activity import CopyActivity
from dotenv import load_dotenv
load_dotenv()


class TestCopyActivity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.postgres_linked_service = LinkedServicePostgres(db_name='postgres',
                                                                host='localhost',
                                                                user='postgres',
                                                                password='postgres',
                                                                port=5432)
            cls.postgres_conn = cls.postgres_linked_service.connect()

            cls.s3_client = boto3.client('s3')
            cls.bucket = 'bucket-bakro'
        except Exception as e:
            print(e)
            print("Connection failed")

    @classmethod
    def tearDownClass(cls):
        cls.postgres_conn.close()

    def test_copy_activity_db_db(self):
        ExecuteSQL(self.postgres_conn).execute( 'CREATE TABLE IF NOT EXISTS people\
                    (user_id VARCHAR(100), language_name VARCHAR(100), level VARCHAR(100))')
        dataset_source = DatasetFromDB(self.postgres_conn, table_name='people')
        dataset_sink = DatasetFromDB(self.postgres_conn, table_name='people_dim')
        mapping = {'id': 'user_id', 'name': 'language_name', 'level': 'level'}
        copy_activity = CopyActivity(dataset_source, dataset_sink, mapping)
        copy_activity.copy()
        # check if the table is created and data is copied
        count = ExecuteSQL(self.postgres_conn, 'SELECT COUNT(*) FROM people_dim').execute()
        # check data size in the table > 0
        self.assertGreater(count, 0)

    def test_copy_activity_db_dl(self):
        source_postgres = DatasetFromDB(self.postgres_conn, table_name='people')
        sink_s3 = DatasetFromDataLake(self.bucket, 's3://bucket-bakro/people.csv', 'csv')
        mapping = {'id': 'user_id', 'name': 'language_name', 'level': 'level'}
        copy_activity = CopyActivity(source_postgres, sink_s3, mapping)
        copy_activity.copy()
        # check if the file is created in the bucket
        response = self.s3_client.list_objects(Bucket=self.bucket, Prefix='people.csv')
        # check if the file is created
        self.assertIn('Contents', response)

    def test_copy_activity_dl_db(self):
        source_s3 = DatasetFromDataLake(self.bucket, 's3://bucket-bakro/people.csv', 'csv')
        sink_postgres = DatasetFromDB(self.postgres_conn, table_name='people_dim')
        mapping = {'id': 'user_id', 'name': 'language_name', 'level': 'level'}
        copy_activity = CopyActivity(source_s3, sink_postgres, mapping)
        copy_activity.copy()
        # check if the table is created and data is copied
        count = ExecuteSQL(self.postgres_conn, 'SELECT COUNT(*) FROM people_dim').execute()
        # check data size in the table > 0
        self.assertGreater(count, 0)

    def test_copy_activity_dl_dl(self):
        source_s3 = DatasetFromDataLake(self.bucket, 's3://bucket-bakro/people.csv', 'csv')
        sink_s3 = DatasetFromDataLake(self.bucket, 's3://bucket-bakro/people_dim.parquet', 'parquet')
        mapping = {'id': 'user_id', 'name': 'language_name', 'level': 'level'}
        copy_activity = CopyActivity(source_s3, sink_s3, mapping)
        copy_activity.copy()
        # check if the file is created in the bucket
        response = self.s3_client.list_objects(Bucket=self.bucket, Prefix='people_dim.parquet')
        # check if the file is created
        self.assertIn('Contents', response)

    def test_copy_api_db(self):
        url = 'http://localhost:8000/api/user_languages'
        source_api = DatasetFromAPI(url)
        sink_postgres = DatasetFromDB(self.postgres_conn, table_name='temp')
        mapping = {'user_id': 'user_id', 'language': 'lang_name', 'level': 'lvl'}
        copy_activity = CopyActivity(source_api, sink_postgres, mapping)
        copy_activity.copy()
        # check if the table is created and data is copied
        datasource = DatasetFromDB(self.postgres_conn, table_name='temp')
        data = datasource.extract()
        # check data size in the table > 0
        self.assertGreater(data.shape[0], 0)


if __name__ == '__main__':
    unittest.main()

