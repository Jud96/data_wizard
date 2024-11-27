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

    def load(self, data):
        cursor = self.conn.cursor()
        try:
            # Insert bulk data to PostgreSQL
            data = data.values.tolist()  # Assuming `self.data` is a DataFrame-like object
            for row in data:
                try:
                    # Use parameterized queries to prevent SQL injection
                    query = sql.SQL('INSERT INTO {} VALUES ({})').format(
                        sql.Identifier(self.table_name),
                        sql.SQL(', ').join(sql.Placeholder() * len(row))
                    )
                    print(query)
                    cursor.execute(query, row)
                except Exception as e:
                    print(f"Error inserting row {row}: {e}")
                    self.conn.rollback()  # Rollback on individual row failure to ensure database consistency
            self.conn.commit()  # Commit after processing all rows
        except Exception as e:
            print(f"Error during data loading: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def load_bulk(self, data):
        cursor = self.conn.cursor()
        
        try:
            # Insert bulk data to PostgreSQL
            data = data.values.tolist()  # Assuming `self.data` is a DataFrame-like object
            query = sql.SQL('INSERT INTO {} VALUES ({})').format(
                sql.Identifier(self.table_name),
                sql.SQL(', ').join(sql.Placeholder() * len(data[0]))
            )
            print(query)
            cursor.executemany(query, data)
            self.conn.commit()  # Commit after processing all rows
        except Exception as e:
            print(f"Error during data loading: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def delta_load(self, data, key_columns):
        """
        Performs a delta load: Inserts new records and updates existing records based on key columns.

        :param dataset: An object containing `conn` (database connection) and `table_name` (target table name).
        :param data: A DataFrame-like object with the data to be loaded.
        :param key_columns: A list of column names to use as unique keys for conflict resolution.
        """
        cursor = self.conn.cursor()
        try:
            # Convert data to a list of lists for efficient processing
            data_list = data.values.tolist()
            columns = data.columns  # Assuming data is a DataFrame with `.columns` attribute

            # Generate SQL components
            insert_columns = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
            placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
            conflict_columns = sql.SQL(", ").join(sql.Identifier(col) for col in key_columns)
            update_assignments = sql.SQL(", ").join(
                sql.Composed([sql.Identifier(col), sql.SQL(" = EXCLUDED."), sql.Identifier(col)])
                for col in columns if col not in key_columns
            )

            # Prepare the query
            query = sql.SQL("""
                INSERT INTO {table} ({insert_columns})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_columns})
                DO UPDATE SET {update_assignments};
            """).format(
                table=sql.Identifier(self.table_name),
                insert_columns=insert_columns,
                placeholders=placeholders,
                conflict_columns=conflict_columns,
                update_assignments=update_assignments
            )

            # Execute the query for each row
            for row in data_list:
                cursor.execute(query, row)

            # Commit the transaction
            self.conn.commit()
        except Exception as e:
            print(f"Error during data loading: {e}")
            self.conn.rollback()  # Rollback the transaction on error
        finally:
            cursor.close()

    def close(self):
        self.conn.close()
