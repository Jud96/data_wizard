import requests
import pandas as pd
from .dataset import Dataset

class DatasetFromAPI(Dataset):
    def __init__(self, url):
        self.url = url

    def extract(self):
        response = requests.get(self.url)
        try:
            data = response.json()
        except Exception as e:
            print(f"Error extracting data: {e}")
            data = []
        return pd.DataFrame(data)

    def load(self,data):
        # load data to the sink
        try:
            response = requests.post(self.url, json=data.to_dict(orient='records'))
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Error during data loading: {e}")
            print(response.text)