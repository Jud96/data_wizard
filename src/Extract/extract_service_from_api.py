from .extract_service import ExtractService
import requests
import pandas as pd

class ExtractServiceFromAPI(ExtractService):
    def __init__(self, url):
        self.url = url

    def extract(self):
        response = requests.get(self.url)
        data = response.json()
        df = pd.DataFrame(data)
        df['source'] = 'api'
        return df