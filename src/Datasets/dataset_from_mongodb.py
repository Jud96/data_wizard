from .dataset import Dataset
import pandas as pd

class MongoDataset(Dataset):

    def __init__(self, collection):
        self.collection = collection

    def extract(self, query=None):
        if query is not None:
            data = self.collection.find(query)
        else:
            data = self.collection.find()
        df = pd.DataFrame(list(data))
        return df

    def load(self, data):
        data = data.to_dict(orient='records')
        self.collection.insert_many(data)

    def load_bulk(self, data):
        data = data.to_dict(orient='records')
        self.collection.insert_many(data)

    def delete(self):
        # delete collection
        self.collection.drop()