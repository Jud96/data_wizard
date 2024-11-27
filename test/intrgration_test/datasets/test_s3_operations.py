from src.LinkedService.linked_service_s3 import LinkedServiceS3
from src.Datasets.dataset_from_datalake import DatasetFromDataLake
import os
import unittest
import pandas as pd
from dotenv import load_dotenv



class TestS3Operations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_dotenv()
        cls.s3_linked_service = LinkedServiceS3(aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                                aws_secret_access_key=os.getenv(
                                                    'AWS_SECRET_ACCESS_KEY'),
                                                bucket_name=os.getenv(
                                                    'BUCKET_NAME'),
                                                region_name=os.getenv('AWS_REGION'))
        cls.conn = cls.s3_linked_service.connect()
        cls.path = f's3://{os.getenv("BUCKET_NAME")}/data/orders.csv'
        cls.dataset = DatasetFromDataLake(cls.conn, cls.path, 'csv')

    def test_get_metadata(self):
        metadata = self.s3_linked_service.get_metadata()
        self.assertIsInstance(metadata, list)
        self.assertTrue(len(metadata) > 0)

    def test_load_extract_from_s3(self):
        # load data to s3
        local_file_path = 'data/orders.csv'
        data = pd.read_csv(local_file_path)
        self.dataset.load(data= data)
        # extract data from s3
        extracted_data = self.dataset.extract()
        print(extracted_data)
        self.assertIsNotNone(extracted_data)
        self.assertTrue(len(extracted_data) > 0)
        metadata = self.s3_linked_service.get_metadata()
        # check if data/orders.csv exists
        is_file_exists = False
        for obj in metadata:
            if obj['Key'] == 'data/orders.csv':
                is_file_exists = True
                break
        self.assertTrue(is_file_exists)
        self.dataset.delete('data/orders.csv')
        metadata = self.s3_linked_service.get_metadata()
        # check if data/orders.csv exists
        is_file_exists = False
        for obj in metadata:
            if obj['Key'] == 'data/orders.csv':
                is_file_exists = True
                break
        self.assertFalse(is_file_exists)


if __name__ == '__main__':
    unittest.main()