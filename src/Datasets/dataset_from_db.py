from .dataset import Dataset
import pandas as pd
from psycopg2 import sql


class DatasetFromDB(Dataset):

    def __init__(self, conn, query=None, table_name=None):
        # if both query and table_name are None, raise an exception
        if query is None and table_name is None:
            raise ValueError("Either query or table_name must be provided")

        if query is not None and table_name is not None:
            raise ValueError(
                "Only one of query or table_name should be provided")

        self.conn = conn
        self.query = query
        self.table_name = table_name

    def extract(self):
        cursor = self.conn.cursor()
        if self.table_name is not None:
            cursor.execute(f"SELECT * FROM {self.table_name}")
        else:
            cursor.execute(self.query)
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data)
        df.columns = column_names
        cursor.close()
        return df

   
