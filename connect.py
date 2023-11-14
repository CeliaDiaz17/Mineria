import getpass
import os
import traceback
import sys
import oracledb
import pandas as pd

un = 'admin'
pw = getpass.getpass(f'Enter password for {un}: ')
schema_name = 'SCHEMA_SILVER'  # Reemplaza 'TUSCHEMA' con el nombre de tu esquema

cs = f'(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.eu-madrid-1.oraclecloud.com))(connect_data=(service_name=g067633159c582f_dbmm_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'

try:
    connection = oracledb.connect(user=un, password=pw, dsn=cs)
    cursor = connection.cursor()

    # Ruta al directorio que contiene los archivos CSV
    csv_directory = 'resultados'

    for csv_file in os.listdir(csv_directory):
        if csv_file.endswith('.csv'):
            file_path = os.path.join(csv_directory, csv_file)

            # Utiliza pandas para leer las columnas del archivo CSV
            df = pd.read_csv(file_path)
            columns = df.columns.tolist()

            # Extrae el nombre de la tabla del nombre del archivo CSV (sin la extensión)
            table_name = os.path.splitext(csv_file)[0]

            # Verifica si la tabla ya existe
            check_table_sql = f"SELECT COUNT(*) FROM ALL_TABLES WHERE TABLE_NAME = '{table_name}' AND OWNER = '{schema_name}'"
            cursor.execute(check_table_sql)
            existing_tables = cursor.fetchall()

            # Si la tabla no existe, créala
            if not existing_tables:
                # Mapeo de tipos de datos entre pandas y Oracle
                oracle_data_types = {
                    'int64': 'NUMBER',
                    'float64': 'FLOAT',
                    'object': 'VARCHAR2(255)'
                }

                # Crear una lista de definiciones de columna
                column_definitions = [
                    f'"{col}" {oracle_data_types[df[col].dtype.name.lower()]}' for col in columns
                ]

                # Crea una tabla para cada archivo CSV en el esquema específico
                create_table_sql = f"""
                CREATE TABLE {schema_name}.{table_name} (
                    {", ".join(column_definitions)}
                )
                """
                cursor.execute(create_table_sql)

                # Inserta datos desde el archivo CSV usando executemany
                insert_sql = f"INSERT INTO {schema_name}.{table_name} VALUES ({', '.join([':' + col for col in columns])})"
                data_to_insert = df.values.tolist()
                cursor.executemany(insert_sql, data_to_insert)
            else:
                print(
                    f"La tabla {table_name} ya existe en el esquema {schema_name}.")

    connection.commit()
    print(
        f"Datos cargados en tablas individuales en el esquema {schema_name}.")


except oracledb.Error as e:
    error, = e.args
    print(error.message)
    traceback.print_tb(sys.exc_info()[2])

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals():
        connection.close()
