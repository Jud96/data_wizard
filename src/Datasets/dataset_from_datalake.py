from .dataset import Dataset
import pandas as pd


class DatasetFromDataLake(Dataset):
    mapping_load = {'csv': 'to_csv',
                    'json': 'to_json',
                    'parquet': 'to_parquet',
                    'excel': 'to_excel',
                    'json': 'to_json'
                    }
    mapping_extract = {'csv': 'read_csv',
                       'json': 'read_json',
                       'parquet': 'read_parquet',
                       'excel': 'read_excel',
                       'json': 'read_json'
                       }

    def __init__(self, conn, path, file_format):
        self.conn = conn
        self.path = path
        self.file_format = file_format

    def extract(self):
        # # retrieve file from data lake and return as a DataFrame
        # file = self.conn.get_file(self.path)
        # extract file type from file name
        file_type = self.path.split('.')[-1]
        df = pd.__getattribute__(self.mapping_extract[file_type])(self.path)
        return df

    def load(self, data):
        # save data to data lake
        # extract file type from file name
        file_type = self.path.split('.')[-1]
        data.__getattribute__(self.mapping_load[file_type])(self.path)

    def delete(self, key):
        try:

            bucket = self.path.split('/')[2]
            print(bucket)
            self.conn.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(e)
            return None
