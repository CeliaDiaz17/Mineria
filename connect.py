import getpass
import os
import traceback
import sys
import oracledb
import pandas as pd

def ddbb_connection_decorator(func):
    def wrapper(*args, **kwargs):
        conexion_db = Connect() # Obtener la instancia única de la conexión a la base de datos

        try:
            conexion_db.open_connection()
            resultado = func(*args, **kwargs)
            return resultado
        
        finally:
            conexion_db.close_connection()

    return wrapper

class Connect:
    _instancia = None
    _config = None

    def __new__(cls, *args, **kwargs):
        if not cls._instancia:
            cls._instancia = super(Connect, cls).__new__(cls, *args, **kwargs)
        return cls._instancia

    def __init__(self):
        self.connection = None
        self.cursor = None

    def open_connection(self, user):
        pw = getpass.getpass(f'Enter password for {user}: ')

        cs = f'(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.eu-madrid-1.oraclecloud.com))(connect_data=(service_name=g067633159c582f_dbmm_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'

        try:
            self.connection = oracledb.connect(user=user, password=pw, dsn=cs)
            self.cursor = self.connection.cursor()
            print(f"Connection established successfully.")

        except oracledb.Error as e:
            error, = e.args
            print(error.message)
            traceback.print_tb(sys.exc_info()[2])
            
    @ddbb_connection_decorator
    def dump_data(self, schema_name, directory):
        csv_directory = directory

        for csv_file in os.listdir(csv_directory):
            if csv_file.endswith('.csv'):
                file_path = os.path.join(csv_directory, csv_file)

                chunk_size = 10000
                chunks = pd.read_csv(file_path, chunksize=chunk_size, low_memory=False)

                table_name = os.path.splitext(csv_file)[0]

                check_table_sql = f"SELECT COUNT(*) FROM ALL_TABLES WHERE TABLE_NAME = '{table_name}' AND OWNER = '{schema_name}'"
                self.cursor.execute(check_table_sql)
                table_exists = self.cursor.fetchall()

                if not table_exists:
                    oracle_data_types = {
                        'int64': 'NUMBER',
                        'float64': 'FLOAT',
                        'object': 'VARCHAR2(255)'
                    }

                    columns = []
                    for chunk in chunks:
                        columns.extend(chunk.columns.tolist())
                        break

                    column_definitions = [
                        f'"{col}" {oracle_data_types[chunk[col].dtype.name.lower()]}' for col in columns
                    ]

                    create_table_sql = f"""
                    CREATE TABLE {schema_name}.{table_name} (
                        {", ".join(column_definitions)}
                    )
                    """
                    self.cursor.execute(create_table_sql)

                    insert_sql = f"INSERT INTO {schema_name}.{table_name} VALUES ({', '.join([':' + col for col in columns])})"
                    for chunk in chunks:
                        data_to_insert = chunk.values.tolist()
                        self.cursor.executemany(insert_sql, data_to_insert)

        self.connection.commit()
        print(f"Data loaded into individual tables in the schema {schema_name}.")

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection closed.")



if __name__ == '__main__':
    """Basic connector for testing, """
    connector = Connect()
    connector.open_connection(user='user_r')
    connector.dump_data(schema_name='SCHEME_RAW', directory='results_raw')
    connector.close_connection()
