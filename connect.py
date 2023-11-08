import getpass
import os
import traceback
import oracledb
import pandas as pd

un = 'user_w'
cs = '(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1521)(host=adb.eu-madrid-1.oraclecloud.com))(connect_data=(service_name=g067633159c582f_dbmm_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'
pw = getpass.getpass(f'Enter password for {un}: ')

try:
    connection = oracledb.connect(user=un, password=pw, dsn=cs)

    with connection.cursor() as cursor:
        # Crear una tabla "raw_data" para almacenar los datos brutos
        create_table_sql = """
        CREATE TABLE raw_data (
            -- Columnas se definirán automáticamente desde el archivo CSV
        )
        """
        cursor.execute(create_table_sql)
        connection.commit()

        # Ruta al directorio que contiene los archivos CSV
        csv_directory = '/csv'

        # Iterar a través de los archivos CSV y cargar los datos en la tabla
        for csv_file in os.listdir(csv_directory):
            if csv_file.endswith('.csv'):
                file_path = os.path.join(csv_directory, csv_file)

                # Utiliza pandas para leer las columnas del archivo CSV
                df = pd.read_csv(file_path)
                columns = df.columns.tolist()

                with open(file_path, 'r') as csv_file:
                    next(csv_file)  # Saltar la primera fila si es una cabecera
                    cursor.copyfrom(
                        csv_file,
                        'raw_data',
                        columns=columns  # Utiliza las columnas leídas automáticamente
                    )

        connection.commit()
        print("Datos cargados exitosamente en la tabla 'raw_data'.")

except oracledb.Error as e:
    error, = e.args
    print(error.message)
    traceback.print_tb(e.__traceback__)

finally:
    connection.close()
