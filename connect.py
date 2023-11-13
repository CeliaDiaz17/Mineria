import getpass
import os
import traceback
import oracledb
import pandas as pd

un = 'user_w'
pw = getpass.getpass(f'Enter password for {un}: ')
schema_name = 'TUSCHEMA'  # Reemplaza 'TUSCHEMA' con el nombre de tu esquema

cs = f'(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1521)(host=adb.eu-madrid-1.oraclecloud.com))(connect_data=(service_name=g067633159c582f_dbmm_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes))(user={un}))(schema={schema_name})'

try:
    connection = oracledb.connect(user=un, password=pw, dsn=cs)
    cursor = connection.cursor()

    # Ruta al directorio que contiene los archivos CSV
    csv_directory = '/csv'

    for csv_file in os.listdir(csv_directory):
        if csv_file.endswith('.csv'):
            file_path = os.path.join(csv_directory, csv_file)

            # Utiliza pandas para leer las columnas del archivo CSV
            df = pd.read_csv(file_path)
            columns = df.columns.tolist()

            # Extrae el nombre de la tabla del nombre del archivo CSV (sin la extensión)
            table_name = os.path.splitext(csv_file)[0]

            # Crea una tabla para cada archivo CSV en el esquema específico
            create_table_sql = f"""
            CREATE TABLE {schema_name}.{table_name} (
                {", ".join([f"{col} VARCHAR2(100)" for col in columns])} 
                -- Define los tipos de datos adecuados
            )
            """
            cursor.execute(create_table_sql)

            with open(file_path, 'r') as csv_file:
                next(csv_file)  # Saltar la primera fila si es una cabecera
                cursor.copyfrom(
                    csv_file,
                    f'{schema_name}.{table_name}',
                    columns=columns
                )

    connection.commit()
    print(
        f"Datos cargados en tablas individuales en el esquema {schema_name}.")

except oracledb.Error as e:
    error, = e.args
    print(error.message)
    traceback.print_tb(e.__traceback())

finally:
    cursor.close()
    connection.close()
