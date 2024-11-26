import pymongo
from src.LinkedService.linked_service_to_db import LinkedServiceToDB


class LinkedServiceToMongoDB(LinkedServiceToDB):
    def __init__(self, db_name, user, password, host, port):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        try:
            conn = pymongo.MongoClient(f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/")
            db_conn = conn[self.db_name]
            print("Connected successfully!!!")
            return db_conn
        except Exception as e:
            print("Could not connect to MongoDB: %s" % e)
            return None

    def get_metadata(self):
        try:
            conn = self.connect()
            collections = conn.list_collection_names()
            metadata = []
            for collection in collections:
                metadata.append({
                    'collection': collection,
                    'count': conn[collection].count_documents({})
                })
            return metadata
        except Exception as e:
            print(f"Error fetching metadata: {e}")
            return None
    
