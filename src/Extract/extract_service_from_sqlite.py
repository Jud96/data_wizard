from .extract_service import ExtractService
import pandas as pd
import sqlite3

class ExtractServiceFromsqlite(ExtractService):
    def __init__(self, db_path, query):
        self.db_path = db_path
        self.query = query

    def extract(self):
        conn = sqlite3.connect(self.db_path)
        data = pd.read_sql_query(self.query, conn)
        data['source'] = 'sql'
        conn.close()
        return data