import csv
import os
import pandas as pd 
import time
import zipfile
import subprocess
import kaggle
from urllib.parse import urlparse
from kaggle.api.kaggle_api_extended import KaggleApi
import shutil
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from pathlib import Path
import getpass
import traceback
import oracledb

# Sera borrado en un futuro cercano ya que solo servia para ciertas comprobaciones previas
def comprobaciones():
    folder_path="csv"
    cont=2005
    for filename in os.listdir(folder_path):
        total_rows_with_values=0
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            # Read the CSV file
            data = csv_to_df(file_path)
            # Check rows
            educacion = data[(data['education_1989_revision'].notnull()) & (data['education_2003_revision'].notnull())]
            suicide=data[(data['manner_of_death']==2)]
            injury = data[(data['manner_of_death']==2) & (data['injury_at_work']=='Y')]
            activity=data[(data['manner_of_death']==2) & (data['activity_code'].notnull())]
            age=data[(data['age_recode_52'].notnull()) & (data['age_recode_27'].notnull()) & (data['age_recode_12'].notnull())]
            cause=data[(data['manner_of_death']==2) & ((data['358_cause_recode']) | (data['113_cause_recode']) | (data['39_cause_recode']))]
            allCause=data[(data['358_cause_recode'].notnull()) & (data['113_cause_recode'].notnull()) & (data['39_cause_recode'].notnull())]
            infantSuicide=data[(data['manner_of_death']==2) & (data['130_infant_cause_recode'].notnull())]
            minus19=data[(data['age_recode_52']<30) & (data['manner_of_death']==2)]
            noEducationRegisterSucide=data[((data['education_1989_revision']==99) | (data['education_2003_revision']==9)) & (data['manner_of_death']==2)]
            # Print data
            numSuicide=len(suicide)
            print(cont)
            print(f"\tNumber of rows with values in 'education_1989_revision' and 'education_2003_revision': {len(educacion)} percentage: {len(educacion)/len(data)*100}%")
            print(f"\tNumber of rows with suicide and 'injury_at_work': {len(injury)} percentage: {len(injury)/numSuicide*100}%")
            print(f"\tNumber of rows with suicide and 'activity_code': {len(activity)} percentage: {len(activity)/numSuicide*100}%")
            print(f"\tNumber of rows with 3 'age_recorde': {len(age)} percentage: {len(age)/len(data)*100}%")
            print(f"\tNumber of rows with suicide and 'cause_recode': {len(cause)} percentage: {len(cause)/numSuicide*100}%")
            print(f"\tNumber of rows with all 'cause_recode': {len(allCause)} percentage: {len(allCause)/len(data)*100}%")
            print(f"\tNumber of rows of infant suicide: {len(infantSuicide)} percentage: {len(infantSuicide)/numSuicide*100}%")
            print(f"\tNumber of less than 19 years old suicide: {len(minus19)} percentage: {len(minus19)/numSuicide*100}%")
            print(f"\tNumber of suicided ppl with no education register: {len(noEducationRegisterSucide)} percentage: {len(noEducationRegisterSucide)/numSuicide*100}%")
            print(f"\tTotal rows: {len(data)}")
            cont+=1

    print(f"\tTotal rows: {len(data)}")

# Lee un archuvo csv pasando su ruta, lo transforma en un dataframe y lo devuelve
# df.astype()
def csv_to_df(ruta):
    nRowsRead = None
    data = pd.read_csv(ruta, delimiter=',', nrows = nRowsRead,low_memory=False)
    data.dataframeName = ruta
    return data

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

# Funcion que descarga cualquier dataset de la pagina de kaggle usando su api indicando el nombre del dataset y la carpeta donde quieres guardarlo
def download_kaggle(download_dir,dataset_name):
    # Descarga con kaggle
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(str(dataset_name), path = str(download_dir), unzip = True)
    os.chmod(download_dir, 0o755)

# Descarga el dataset de la pagina de cdc.gov haciendo uso de selenium, necesita el enlace de la pagina y el directorio de descarga   
# Mirar headless 
def download_suicide_rate(download_dir, url):
    options = Options()
    options.add_argument("-headless") 
    # Use GeckoDriverManager to automatically download the compatible GeckoDriver for Firefox
    driver = webdriver.Firefox(options=options)
    driver.get(url)
    #time.sleep(5)  # Give the page some time to load

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
# VCP
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

# Menu con las distintas opciones del programa
def menu(columns_removed):
    # Mortality dataset info
    download_dir_mortalidad = "csv/mortalidad/"
    dataset_mortalidad = "cdc/mortality"
    # Suicide rate dataset info
    download_dir_suicide_rate = "csv/suicide_rate/"
    dataset__suicide_rate_url = "https://www.cdc.gov/nchs/pressroom/sosmap/suicide-mortality/suicide.htm"
    # Unemployment dataset info
    download_dir_unemployment = "csv/unemployment/"
    dataset_unemployment = "aniruddhasshirahatti/us-unemployment-dataset-2010-2020"

    while True:
        opcion = input("Opciones:\n1. Descargar datos de 'mortalidad'\n2. Descargar datos de 'suicide rate'\n3. Descargar datos de 'unemployment data'\n4. Crear capa RAW 'mortalidad'\n5. Crear capa RAW 'suicide rate'\n6. Crear capa RAW 'unemployment data'\n7. Crear capa SILVER de 'mortalidad' desde RAW (out of memory)\n8. Crear capa SILVER de 'mortalidad' optimizado\n9. Crear capa SILVER de 'suicide rate'\n10. Crear capa SILVER de 'unemployment data'\n11. Eliminar archivos csv's\n12. Conexion con la base de datos\n13. Salir\nSelecciona una opción: ")

        # Opcion 1: Descarga del dataset cdc/mortality de kaggle 
        if opcion == "1":
            print("Inicio de la descarga del data set", dataset_mortalidad)
            download_kaggle(download_dir_mortalidad, dataset_mortalidad)
            for filename in os.listdir(download_dir_mortalidad):
                if filename.endswith(".json"):
                    file_path = os.path.join(download_dir_mortalidad, filename)
                    os.remove(file_path)
            
            print('Fin de la descraga de', dataset_mortalidad,'\n')
        
        # Opcion 2: Descarga del dataset suicide_rate de cdc.gov 
        elif opcion == "2":
            print("Inicio de la descarga de SuicideRateData")
            download_suicide_rate(download_dir_suicide_rate, dataset__suicide_rate_url)
            print('Fin de la descraga de SuicideRateData\n')
            
        # Opcion 3: Descarga del dataset unemployment de kaggle 
        elif opcion == "3":
            print("Inicio de la descarga de UnemploymentData")
            download_kaggle(download_dir_unemployment, dataset_unemployment)
            os.remove('csv/unemployment/unemployment_data_us_state.csv')
            print('Fin de la descraga de UnemploymentData\n')

        # Opcion 4: Creacion de la capa RAW del dataset de mortalidad    
        elif opcion == '4':
            print("Inicio de la creacion del archivo 'raw_mortalidad_data'")
            raw_df_mortalidad = join_csvs(download_dir_mortalidad)
            save_csv(raw_df_mortalidad, 'raw_mortalidad_data.csv')
            print('Archivo raw creado\n')

        # Opcion 5: Creacion de la capa RAW del dataset suicide rate
        elif opcion == '5':
            print("Inicio de la creacion del archivo 'raw_suicide_rate'")
            print(os.listdir(download_dir_suicide_rate)[0])
            ruta = os.path.join(download_dir_suicide_rate, os.listdir(download_dir_suicide_rate)[0])
            raw_df_suicide_rate = csv_to_df(ruta)
            save_csv(raw_df_suicide_rate, 'raw_suicide_rate_data.csv')
            print('Archivo raw creado\n')
        
        # Opcion 6: Creacion de la capa RAW del dataset unemployment
        elif opcion == '6':
            print("Inicio de la creacion del archivo 'raw_unemployment_data'")
            raw_df_unemployment_data = csv_to_df(os.path.join(download_dir_suicide_rate, os.listdir(download_dir_suicide_rate)[0]))
            save_csv(raw_df_unemployment_data, 'raw_unemployment_data.csv')
            print('Archivo raw creado\n')

        # Opcion 7: Creacion de la capa SILVER del dataset mortalidad desde la capa RAW (out of memory)
        elif opcion == '7':
            print("Inicio del preprocesamiento del dataset de 'mortalidad'")
            preprocess_suicide_data = prepocessiong_suicide_data_unitary('resultados/raw_data_mortalidad.csv', columns_removed)
            save_csv(preprocess_suicide_data, "silver_suicide_data.csv")
            print("Fin del guardado de datos en el archivo silver_suicide_data.csv\n")

        # Opcion 8: Creacion de la capa SILVER del dataset mortalidad
        elif opcion == '8':
            print("Inicio del preprocesamiento del dataset de 'mortalidad'")
            preprocess_suicide_data = preprocessing_suicide_data_group(download_dir_mortalidad, columns_removed)
            print("Inicio del guardado de datos...")
            save_csv(preprocess_suicide_data, "silver_suicide_data.csv")
            print("Fin del guardado de datos en el archivo silver_suicide_data.csv\n")
        
        # Opcion 9: Creacion de la capa SILVER del dataset suicide rate
        elif opcion == '9':
            print("Inicio del preprocesamiento del dataset de 'suicide rate'")
            preprocess_suicide_rate_data = preprocessing_suicide_rate_or_unemployment_data(1, download_dir_suicide_rate)
            print("Inicio del guardado de datos...")
            save_csv(preprocess_suicide_rate_data, "silver_suicide_rate_data.csv")
            print("Fin del guardado de datos en el archivo suicide_rate_data.csv\n")

        # Opcion 10: Creacion de la capa SILVER del dataset unemployment
        elif opcion == '10':
            print("Inicio del preprocesamiento del dataset de 'unemployment'")
            preprocess_data = preprocessing_suicide_rate_or_unemployment_data(2,download_dir_unemployment)
            print("Inicio del guardado de datos...")
            save_csv(preprocess_data, "silver_unemployment_data.csv")
            print("Fin del guardado de datos en el archivo unemployment_data.csv\n")

        # Opcion para eliminar los csv's del proyecto
        elif opcion == "11":
            op='0'
            while op >'4' or op <'1':
                op=input("\t1. Eliminar el directorio 'csv'\n\t2. Eliminar el directorio 'resultados'\n\t3. Eliminar ambos\n\t4. Salir\n\tIntroduzca una opcion: ")
                if op == '1': delete_csv(["csv"])
                elif op == '2': delete_csv(["resultados"])
                elif op == '3': delete_csv(["csv","resultados"])
                elif op == '4': print()
                else: print(f"Opción no válida. Por favor, selecciona una opción válida.")

        # Opcion 12: Conexion con la base de datos
        elif opcion == "12":
            print("Iniciando conexion con la base de datos...")
            cnx = connect_ddbb()
            print(cnx)
            cnx.close()

        # Opcion 13: Salida
        elif opcion == "13":
            break
        else:
            print("Opción no válida. Por favor, selecciona una opción válida.")


if __name__ == '__main__':
    columnas_eliminadas = ["infant_age_recode_22","130_infant_cause_recode","method_of_disposition", "autopsy", "icd_code_10th_revision", "number_of_entity_axis_conditions", "entity_condition_1", "entity_condition_2", "entity_condition_3", "entity_condition_4", "entity_condition_5", "entity_condition_6", "entity_condition_7", "entity_condition_8", "entity_condition_9", "entity_condition_10", "entity_condition_11", "entity_condition_12", "entity_condition_13", "entity_condition_14", "entity_condition_15", "entity_condition_16", "entity_condition_17", "entity_condition_18", "entity_condition_19", "entity_condition_20", "number_of_record_axis_conditions", "record_condition_1", "record_condition_2", "record_condition_3", "record_condition_4", "record_condition_5", "record_condition_6", "record_condition_7", "record_condition_8", "record_condition_9", "record_condition_10", "record_condition_11", "record_condition_12", "record_condition_13", "record_condition_14", "record_condition_15", "record_condition_16", "record_condition_17", "record_condition_18", "record_condition_19", "record_condition_20","age_recode_27","age_recode_12"]
    menu(columnas_eliminadas)

    