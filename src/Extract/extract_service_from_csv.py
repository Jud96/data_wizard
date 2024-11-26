from extract_service import ExtractService
import pandas as pd

class ExtractServiceFromCSV(ExtractService):
    def __init__(self, file_path):
        self.file = file_path
    def extract(self):
        df = pd.read_csv(self.file)
        df['source'] = 'csv'
        return df