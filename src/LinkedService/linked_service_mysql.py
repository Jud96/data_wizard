import mysql.connector
from src.LinkedService.linked_service_to_db import LinkedServiceToDB


class LinkedServiceMySQL(LinkedServiceToDB):
    def __init__(self, db_name, user, password, host, port):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        try:
            conn = mysql.connector.connect(
                database=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except mysql.connector.Error as e:
            print(f"Error connecting to the database: {e}")
            raise

    def get_metadata(self):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
            """)
            metadata = cursor.fetchall()
            cursor.close()
            conn.close()
            return metadata
        except mysql.connector.Error as e:
            print(f"Error fetching metadata: {e}")
            raise
