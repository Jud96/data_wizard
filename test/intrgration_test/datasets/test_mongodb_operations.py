from src.LinkedService.linked_service_to_mongodb import LinkedServiceToMongoDB
from src.Datasets.dataset_from_mongodb import MongoDataset
import pandas as pd
import unittest
import os

class TestMongoDBOperations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.linked_sercice_mongodb = LinkedServiceToMongoDB(db_name=os.environ['MONGODB_DB_NAME'],
                                                            user=os.environ['MONGODB_USER'],
                                                            password=os.environ['MONGODB_PASSWORD'],
                                                            host=os.environ['MONGODB_HOST'],
                                                            port='27017')
        cls.mongodb_conn = cls.linked_sercice_mongodb.conn
        cls.collection = cls.mongodb_conn['test_collection']
        cls.mongo_dataset = MongoDataset(cls.collection)
        cls.df = pd.read_csv('data/orders.csv')

    def test_operations_on_mongodb(self):
        # insert data to mongodb
        self.mongo_dataset.load_bulk(self.df)
        self.assertEqual(len(self.linked_sercice_mongodb.get_metadata()), 1)
        data = self.mongo_dataset.extract()
        self.assertEqual(len(data), 5)
        self.mongo_dataset.delete()
        self.assertEqual(len(self.linked_sercice_mongodb.get_metadata()), 0)