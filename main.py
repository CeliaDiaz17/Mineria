import csv
import os
import pandas as pd 
import time
import zipfile
import kaggle
import getpass
import traceback
import oracledb
import shutil
from urllib.parse import urlparse
from kaggle.api.kaggle_api_extended import KaggleApi
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from pathlib import Path
from sqlalchemy import create_engine

# Lee un archuvo csv pasando su ruta, lo transforma en un dataframe y lo devuelve
def csv_to_df(ruta):
    nRowsRead = None
    data = pd.read_csv(ruta, delimiter=',', nrows = nRowsRead,low_memory=False)
    data.dataframeName = ruta
    return data

# Convierte los tipos de un data frame a unos especificos
def convert_df_datatypes(df, types):
    for col, tipo in types.items():
        df[col] = df[col].astype(tipo)
    return df

# Funcion que descarga cualquier dataset de la pagina de kaggle usando su api indicando el nombre del dataset y la carpeta donde quieres guardarlo
def download_kaggle(download_dir,dataset_name):
    # Descarga con kaggle
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(str(dataset_name), path = str(download_dir), unzip = True)
    os.chmod(download_dir, 0o755)

def download_mortalidad(download_dir, dataset):
    print("Inicio de la descarga del data set", dataset)
    download_kaggle(download_dir, dataset)
    for filename in os.listdir(download_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(download_dir, filename)
            os.remove(file_path)     
    print('Fin de la descraga de', dataset,'\n')

def download_unemployment_data(download_dir, dataset):
    print("Inicio de la descarga de UnemploymentData")
    download_kaggle(download_dir, dataset)
    os.remove('csv/unemployment/unemployment_data_us_state.csv')
    print('Fin de la descraga de UnemploymentData\n')

# Descarga el dataset de la pagina de cdc.gov haciendo uso de selenium, necesita el enlace de la pagina y el directorio de descarga   
def download_suicide_rate(download_dir, url):
    print("Inicio de la descarga de SuicideRateData")
    options = Options()
    options.add_argument("-headless") 
    driver = webdriver.Firefox(options=options)
    driver.get(url)
    time.sleep(5)  # Give the page some time to load

    wait = WebDriverWait(driver, 5)  # You may adjust the timeout as needed

    # Locate the download button element
    download_button = None
    try:
        xpath = '/html/body/div[3]/main/div[3]/div/div[3]/div/div/div[1]/div/div/section/section[3]/span/a'
        download_button = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    except:
        pass

    if download_button:
        # Click the download link
        download_button.click()
        # Wait for the download to complete (you may need to adjust the wait time)
        time.sleep(5)

        downloaded_file = os.path.join(os.path.expanduser('~'), 'Downloads', 'data-table.csv').replace('\\', '/')
        destination_file = Path(os.path.join(download_dir, 'suicide_rate.csv').replace('\\', '/'))

        # Ensure the destination directory exists
        os.makedirs(download_dir, exist_ok=True)

        # Move the file to the destination with the new name
        shutil.move(downloaded_file, destination_file)
        destination_file.chmod(0o644)

    driver.quit()
    print('Fin de la descraga de SuicideRateData\n')

# Une los csv's 
def join_csvs(folder_path):
    data = pd.DataFrame()
    cont = 1
    for filename in os.listdir(folder_path):
        print('Archivo', cont)
        cont += 1
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            try:
                data_temp = (csv_to_df(file_path))
                data = pd.concat([data, data_temp], ignore_index=True)
            except pd.errors.EmptyDataError:
                print(f"El archivo {filename} está vacío.")

    return data

# Lee un archivo csv lo pasa a dataframe y realiza el procesamiento necesario para cerar la capa silver. Devuelve un dataframe            
def prepocessiong_suicide_data_unitary(fodler_path, columns_removed):
    try:
        data_temp = csv_to_df(fodler_path)
        # Eliminacion de filas
        data_temp = data_temp[data_temp['manner_of_death']==2]
        #print(f"Filas tras la seleccion de suicidios: {len(dataTemp)}")
        data_temp = data_temp[data_temp['130_infant_cause_recode'].isnull()]
        #print(f"Filas tras la eliminacion de los niños: {len(dataTemp)}")

        # Eliminacion de columnas no utiles
        columns_corrected_removed = [col for col in columns_removed if col in data_temp.columns.to_list()]
        data_temp.drop(columns_corrected_removed, axis = 1, inplace = True)

        # Define un diccionario de mapeo para realizar la sustitución de valores
        mapeo_valores = {1: 8, 2: 19, 3: 20, 4: 21, 5: 22, 6: 23, 7: 24, 8: 25, 9: 26}
        # Utiliza la función 'replace' para aplicar el mapeo a la columna 'education_2003_revision' y une las columnas del 1989 y 2003 
        data_temp['education_2003_revision'] = data_temp['education_2003_revision'].replace(mapeo_valores)
        data_temp['education'] = data_temp['education_1989_revision'].fillna(data_temp['education_2003_revision'])

        # Eliminacion de columnas despues de combinarlas en una nueva
        data_temp.drop(['education_1989_revision', 'education_2003_revision'], axis=1, inplace=True)
    except pd.errors.EmptyDataError:
                print(f"El archivo está vacío.")
    
    return data_temp
    
# Lee todos los archivos csv's de una carpeta dada, los tranforma en dataframe, los unes, y se realiza un procesamiento sobre los datos eliminando y unificando ciertas columnas
def preprocessing_suicide_data_group(folder_path, columns_removed):
    cont = 2005
    data = pd.DataFrame()
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            data_temp = prepocessiong_suicide_data_unitary(file_path, columns_removed)            
            data = pd.concat([data, data_temp], ignore_index=True)
            print("Fin de la limpieza del año: ", str(cont),"\n")
            cont+=1

    print(len(data))
    return data

# Lee un archivo csv, lo pasa a un dataframe y selecciona los datos que se encuentran entre el 2005 y 2015 solo para el unployment dataset
def preprocessing_unemployment_data(path):
    folder_path = path + "/unemployment_data_us.csv"
    data = csv_to_df(folder_path)
    data = data[(data['Year']>=2005) & (data['Year']<=2015)]
    return data

# Lee un archivo csv, lo pasa a un dataframe y selecciona los datos que se encuentran entre el 2005 y 2015 solo para suicide_rate dataset
def preprocessing_suicide_rate_data(folder_path, file_name):
    data = csv_to_df(folder_path+file_name)
    data = data[(data['YEAR']>=2005) & (data['YEAR']<=2015)]
    return data

# Lee un archivo csv, lo pasa a un dataframe y selecciona los datos que se encuentran entre el 2005 y 2015 de unployment o suicide_rate dataset
def preprocessing_suicide_rate_or_unemployment_data(op, folder_path):
    for file_name in os.listdir(folder_path):
        data = csv_to_df(folder_path+file_name)
        if op == 1:
            data = data[(data['YEAR']>=2005) & (data['YEAR']<=2015)]
        if op == 2:
            data = data[(data['Year']>=2005) & (data['Year']<=2015)]
        else: print("Opcion incorrecta, ha de ser 1 o 2")
        break
    return data

# Guarda un dataframe en un csv
def save_csv(dataframe, nombre_archivo):
    folder_path = 'resultados/'
    # Check if the folder exists, and create it if it doesn't
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    # Save the DataFrame to the CSV file
    dataframe.to_csv(folder_path+nombre_archivo, index=False)

# Elimina todos los archivos csv's del proyecto
def delete_csv(dir_list):
    for dir in dir_list:
        try:
            shutil.rmtree(dir)
        except FileNotFoundError:
            print(f"El directorio {dir}, no existe")
    
# Conexion con la base de datos
def connect_ddbb():
    un = 'user_r'
    cs = '(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1521)(host=adb.eu-madrid-1.oraclecloud.com))(connect_data=(service_name=g067633159c582f_dbmm_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'
    pw = getpass.getpass(f'Enter password for {un}: ')
    try:
        connection = oracledb.connect(user=un, password=pw, dsn=cs)
        with connection.cursor() as cursor:
            sql = """select systimestamp from dual"""
            for r, in cursor.execute(sql):
                print(r)
    except oracledb.Error as e:
        error, = e.args
        print(error.message)
        traceback.print_tb(e.__traceback__)

    return connection

# Lee el archivo de configuracion de la bbdd y devuleve una lisat con los parametros
def read_config(file):
    configuracion = {}
    with open(file, 'r') as f:
        for linea in f:
            # Dividir la línea en clave y valor
            clave, valor = linea.strip().strip(",").split('=')
            # Quitar comillas y espacios de los valores
            valor = valor.strip().strip("'")
            configuracion[clave] = valor
    
    # Crear una lista ordenada de los valores
    lista_valores = [configuracion['user'], configuracion['password'], configuracion['host'], configuracion['database']]
    return lista_valores

def upload_data(cnx, df, table_name):
    config = read_config(config)
    # Reemplaza los valores entre corchetes con tus propias credenciales e información de la base de datos
    oracle_connection_string = f"oracle+cx_oracle://{config['usuario']}:{config['contraseña']}@{config['host']}:{config['puerto']}/{config['nombre_base_datos']}"
    engine = create_engine(oracle_connection_string)
    # Utiliza el método to_sql para escribir el DataFrame en la tabla 'ofertasLaborales'
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    return 1

def create_raw_mortalidad_data(download_dir):
    print("Inicio de la creacion del archivo 'raw_mortalidad_data'")
    raw_df_mortalidad = join_csvs(download_dir)
    save_csv(raw_df_mortalidad, 'raw_mortalidad_data.csv')
    print('Archivo raw creado\n')
    return 1

def create_raw_sucide_rate_data(download_dir):
    print("Inicio de la creacion del archivo 'raw_suicide_rate'")
    print(os.listdir(download_dir)[0])
    ruta = os.path.join(download_dir, os.listdir(download_dir)[0])
    raw_df_suicide_rate = csv_to_df(ruta)
    save_csv(raw_df_suicide_rate, 'raw_suicide_rate_data.csv')
    print('Archivo raw creado\n')
    return 1

def create_raw_unemployment_data(download_dir):
    print("Inicio de la creacion del archivo 'raw_unemployment_data'")
    raw_df_unemployment_data = csv_to_df(os.path.join(download_dir, os.listdir(download_dir)[0]))
    save_csv(raw_df_unemployment_data, 'raw_unemployment_data.csv')
    print('Archivo raw creado\n')
    return 1

def create_silver_mortalidad_data(download_dir):
    columns_removed = ["infant_age_recode_22","130_infant_cause_recode","method_of_disposition", "autopsy", "icd_code_10th_revision", "number_of_entity_axis_conditions", "entity_condition_1", "entity_condition_2", "entity_condition_3", "entity_condition_4", "entity_condition_5", "entity_condition_6", "entity_condition_7", "entity_condition_8", "entity_condition_9", "entity_condition_10", "entity_condition_11", "entity_condition_12", "entity_condition_13", "entity_condition_14", "entity_condition_15", "entity_condition_16", "entity_condition_17", "entity_condition_18", "entity_condition_19", "entity_condition_20", "number_of_record_axis_conditions", "record_condition_1", "record_condition_2", "record_condition_3", "record_condition_4", "record_condition_5", "record_condition_6", "record_condition_7", "record_condition_8", "record_condition_9", "record_condition_10", "record_condition_11", "record_condition_12", "record_condition_13", "record_condition_14", "record_condition_15", "record_condition_16", "record_condition_17", "record_condition_18", "record_condition_19", "record_condition_20","age_recode_27","age_recode_12"]
    print("Inicio del preprocesamiento del dataset de 'mortalidad'")
    preprocess_suicide_data = preprocessing_suicide_data_group(download_dir, columns_removed)
    print("Inicio del guardado de datos...")
    save_csv(preprocess_suicide_data, "silver_suicide_data.csv")
    print("Fin del guardado de datos en el archivo silver_suicide_data.csv\n")

    return 1

def create_silver_suicide_rate_data(download_dir):
    print("Inicio del preprocesamiento del dataset de 'suicide rate'")
    preprocess_suicide_rate_data = preprocessing_suicide_rate_or_unemployment_data(1, download_dir)
    print("Inicio del guardado de datos...")
    save_csv(preprocess_suicide_rate_data, "silver_suicide_rate_data.csv")
    print("Fin del guardado de datos en el archivo suicide_rate_data.csv\n")
    
    return 1

def create_silver_unemployment_data(download_dir):
    print("Inicio del preprocesamiento del dataset de 'unemployment'")
    preprocess_data = preprocessing_suicide_rate_or_unemployment_data(2,download_dir)
    print("Inicio del guardado de datos...")
    save_csv(preprocess_data, "silver_unemployment_data.csv")
    print("Fin del guardado de datos en el archivo silver_unemployment_data.csv\n")
    
    return 1

def delete_csvs_menu():
    op='0'
    while op >'4' or op <'1':
        op=input("\t1. Eliminar el directorio 'csv'\n\t2. Eliminar el directorio 'resultados'\n\t3. Eliminar ambos\n\t4. Salir\n\tIntroduzca una opcion: ")
        if op == '1': delete_csv(["csv"])
        elif op == '2': delete_csv(["resultados"])
        elif op == '3': delete_csv(["csv","resultados"])
        elif op == '4': print()
        else: print(f"Opción no válida. Por favor, selecciona una opción válida.")
        
def menu():
    # Quizas seria buena idea guardar esta informacion en un txt y asi poder modificarlo en caso de ser necesario sin modificar el codigo
    # Mortality dataset info
    download_dir_mortalidad = "csv/mortalidad/"
    dataset_mortalidad = "cdc/mortality"
    # Suicide rate dataset info
    download_dir_suicide_rate = "csv/suicide_rate/"
    dataset_suicide_rate_url = "https://www.cdc.gov/nchs/pressroom/sosmap/suicide-mortality/suicide.htm"
    # Unemployment dataset info
    download_dir_unemployment = "csv/unemployment/"
    dataset_unemployment = "aniruddhasshirahatti/us-unemployment-dataset-2010-2020"

    while True:
        opcion = input("Opciones:\n1. Menu descragra de datasets\n2. Menu creacion capa RAW\n3. Menu creacion capa SILVER\n97. Eliminar archivos csv's\n98. Conexion con la base de datos\n99. Salir\nSelecciona una opción: ")   
        # Submenu para la descarga
        if opcion == '1':
            while True:
                menu_op = input("\tOpciones:\n\t1. Descargar datos de 'mortalidad'\n\t2. Descargar datos de 'suicide rate'\n\t3. Descargar datos de 'unemployment data'\n\t4. Salir\n\tSeleccione una opcion: ")
                if menu_op == '1': download_mortalidad(download_dir_mortalidad, dataset_mortalidad)
                elif menu_op == '2': download_suicide_rate(download_dir_suicide_rate, dataset_suicide_rate_url)
                elif menu_op == '3': download_unemployment_data(download_dir_unemployment, dataset_unemployment)
                elif menu_op == '4': break
                else: print("Opción no válida. Por favor, selecciona una opción válida.")

        # Submenu para la capa RAW
        elif opcion == '2':
            while True:
                menu_op = input("\tOpciones:\n\t1. Crear capa RAW 'mortalidad\n\t2. Crear capa RAW 'suicide rate'\n\t3. Crear capa RAW 'unemployment data'\n\t4. Salir\n\tSeleccione una opcion: ")
                if menu_op == '1': create_raw_mortalidad_data(download_dir_mortalidad)
                elif menu_op == '2': create_raw_sucide_rate_data(download_dir_suicide_rate)
                elif menu_op == '3': create_raw_unemployment_data(download_dir_unemployment)
                elif menu_op == '4': break
                else: print("Opción no válida. Por favor, selecciona una opción válida.")
        # Submenu para la capa SILVER
        elif opcion == '3':
            while True:
                menu_op = input("\tOpciones:\n\t1. Crear capa SILVER 'mortalidad\n\t2. Crear capa SILVER 'suicide rate'\n\t3. Crear capa SILVER 'unemployment data'\n\t4. Salir\n\tSeleccione una opcion: ")
                if menu_op == '1': create_silver_mortalidad_data(download_dir_mortalidad) 
                elif menu_op == '2': create_silver_suicide_rate_data(download_dir_suicide_rate)
                elif menu_op == '3': create_silver_unemployment_data(download_dir_unemployment)
                elif menu_op == '4': break
                else: print("Opción no válida. Por favor, selecciona una opción válida.")

        # Opcion : Creacion de la capa GOLD 
        elif opcion == '12':
                print()
        # Opcion para eliminar los csv's del proyecto
        elif opcion == "97":
            delete_csvs_menu()
        # Opcion 12: Conexion con la base de datos
        elif opcion == "98":
            print("Iniciando conexion con la base de datos...")
            cnx = connect_ddbb()
            print(cnx)
            cnx.close()
        # Opcion 13: Salida
        elif opcion == "99":
            break
        
if __name__ == '__main__':
    menu()

    