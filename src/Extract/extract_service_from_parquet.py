from .extract_service import ExtractService
import pandas as pd

class ExtractServiceFromParquet(ExtractService):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        data = pd.read_parquet(self.file_path)
        data['source'] = 'parquet'
        return data