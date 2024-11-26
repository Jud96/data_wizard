from  extract_service import ExtractService
import pandas as pd

class ExtractServiceFromDB(ExtractService):
    def __init__(self, conn, query):
        self.conn = conn
        self.query = query

    def extract(self):
        cursor = self.conn.cursor()
        cursor.execute(self.query)
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data)
        df.columns = column_names
        cursor.close()
        return df