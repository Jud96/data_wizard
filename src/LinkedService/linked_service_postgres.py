import psycopg2
from psycopg2 import sql
from LinkedService.linked_service_to_db import LinkedServiceToDB


class LinkedServicePostgres(LinkedServiceToDB):
    def __init__(self, db_name, user, password, host, port):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        try:
            conn = psycopg2.connect(
                database=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            raise

    def get_metadata(self):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_schema, table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name, ordinal_position;
            """)
            metadata = cursor.fetchall()
            cursor.close()
            conn.close()
            return metadata
        except Exception as e:
            print(f"Error fetching metadata: {e}")
            raise
