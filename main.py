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
def csv_to_df(ruta):
    nRowsRead = None
    data = pd.read_csv(ruta, delimiter=',', nrows = nRowsRead,low_memory=False)
    data.dataframeName = ruta
    return data

# Lee todos los archivos csv's de una carpeta dada, los tranforma en dataframe, los unes, y se realiza un procesamiento sobre los datos eliminando y unificando ciertas columnas
def preprocessing_suicide_data(folder_path, columnasEliminadas):
    cont = 2005
    data = pd.DataFrame()
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            try:
                print("Inicio de la limpieza del año:", str(cont))
                dataTemp = (csv_to_df(file_path))

                # Eliminacion de filas
                dataTemp = dataTemp[dataTemp['manner_of_death']==2]
                #print(f"Filas tras la seleccion de suicidios: {len(dataTemp)}")
                dataTemp = dataTemp[dataTemp['130_infant_cause_recode'].isnull()]
                #print(f"Filas tras la eliminacion de los niños: {len(dataTemp)}")

                # Eliminacion de columnas no utiles
                columnasEl = [col for col in columnasEliminadas if col in dataTemp.columns.to_list()]
                dataTemp.drop(columnasEl, axis = 1, inplace = True)

                # Define un diccionario de mapeo para realizar la sustitución de valores
                mapeo_valores = {1: 8, 2: 19, 3: 20, 4: 21, 5: 22, 6: 23, 7: 24, 8: 25, 9: 26}
                # Utiliza la función 'replace' para aplicar el mapeo a la columna 'education_2003_revision' y une las columnas del 1989 y 2003 
                dataTemp['education_2003_revision'] = dataTemp['education_2003_revision'].replace(mapeo_valores)
                dataTemp['education'] = dataTemp['education_1989_revision'].fillna(dataTemp['education_2003_revision'])

                # Eliminacion de columnas despues de combinarlas en una nueva
                dataTemp.drop(['education_1989_revision', 'education_2003_revision'], axis=1, inplace=True)

                # Concatenacion en el data frame final
                data = pd.concat([data, dataTemp], ignore_index=True)

            except pd.errors.EmptyDataError:
                print(f"El archivo {filename} está vacío.")
            print("Fin de la limpieza del año: ", str(cont),"\n")
            cont+=1

    shutil.rmtree(folder_path)
    return data

# Lee un archivo csv, lo pasa a un dataframe y selecciona los datos que se encuentran entre el 2005 y 2015 solo para el unployment dataset
def preprocessing_unemployment_data(path):
    folder_path = path + "/unemployment_data_us.csv"
    data = csv_to_df(folder_path)
    data = data[(data['Year']>=2005) & (data['Year']<=2015)]
    shutil.rmtree(path)
    return data

# Lee un archivo csv, lo pasa a un dataframe y selecciona los datos que se encuentran entre el 2005 y 2015 solo para suicide_rate dataset
def preprocessing_suicide_rate_data(folder_path, file_name):
    data = csv_to_df(folder_path+file_name)
    data = data[(data['YEAR']>=2005) & (data['YEAR']<=2015)]
    shutil.rmtree(folder_path)
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
    shutil.rmtree(folder_path)
    return data

# Funcion que descarga cualquier dataset de la pagina de kaggle usando su api indicando el nombre del dataset y la carpeta donde quieres guardarlo
def download_kaggle(download_dir,dataset_name):
    # Descarga con kaggle
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(str(dataset_name), path = str(download_dir), unzip = True)

# Descarga el dataset de la pagina de cdc.gov haciendo uso de selenium, necesita el enlace de la pagina y el directorio de descarga    
def download_suicide_rate(download_dir, url):
    # Use GeckoDriverManager to automatically download the compatible GeckoDriver for Firefox
    driver = webdriver.Firefox()
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
        destination_file = Path(os.path.join(download_dir, 'suicideRate.csv').replace('\\', '/'))

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

# Conexion con la base de datos
def connect_ddbb():
    un = 'admin'
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
def menu(columnas_eliminadas):
    while True:
        opcion = input("Opciones:\n1. Descargar datos de mortalidad\n2. Procesar datos de SuicideRate\n3. Procesar datos de UnemploymentData\n4. Eliminar archivos de la carpeta csv y resultados\n9. Salir\nSelecciona una opción: ")

        # Opcion 1: Descarga, transformacion a dataframe y preprocesamiento del dataset cdc/mortality de kaggle
        if opcion == "1":
            download_dir_mortalidad = "csv/mortalidad/"
            dataset_mortalidad = "cdc/mortality"
            print("Inicio de la descarga del data set", dataset_mortalidad)
            download_kaggle(download_dir_mortalidad, dataset_mortalidad)
            print("Inicio de la unión de CSVs para generar SuicideData")
            preprocess_suicide_data = preprocessing_suicide_data(download_dir_mortalidad, columnas_eliminadas)
            print("Inicio del guardado de datos...")
            save_csv(preprocess_suicide_data, "suicide_data.csv")
            print("Fin del guardado de datos en el archivo suicid_data.csv")
        
        # Opcion 2: Descarga, transformacion a dataframe y preprocesamiento del dataset suicide_rate de cdc.gov
        elif opcion == "2":
            download_dir_suicide_rate = "csv/suicideRate/"
            dataset__suicide_rate_url = "https://www.cdc.gov/nchs/pressroom/sosmap/suicide-mortality/suicide.htm"
            print("Inicio del procesamiento de SuicideRateData")
            download_suicide_rate(download_dir_suicide_rate, dataset__suicide_rate_url)
            preprocess_suicide_rate_data = preprocessing_suicide_rate_or_unemployment_data(1, download_dir_suicide_rate)
            print("Inicio del guardado de datos...")
            save_csv(preprocess_suicide_rate_data, "suicide_rate_data.csv")
            print("Fin del guardado de datos en el archivo suicide_rate_data.csv")
            
        # Opcion 3: Descarga, transformacion a dataframe y preprocesamiento del dataset unemployment de kaggle
        elif opcion == "3":
            download_dir_unemployment = "csv/unemployment/"
            dataset_unemployment = "aniruddhasshirahatti/us-unemployment-dataset-2010-2020"
            print("Inicio del procesamiento de UnemploymentData")
            download_kaggle(download_dir_unemployment, dataset_unemployment)
            preprocess_data = preprocessing_suicide_rate_or_unemployment_data(2,download_dir_unemployment)
            print("Inicio del guardado de datos...")
            save_csv(preprocess_data, "unemployment_data.csv")
            print("Fin del guardado de datos en el archivo unemployment_data.csv")

        # Opcion para eliminar todos los csv's
        elif opcion == "4":
            print('Eliminando archivos de la carpeta csv y resultados...')
            shutil.rmtree("csv")
            shutil.rmtree("resultados")
            print('Archivos eliminados')
        
        elif opcion == "5":
            print("Iniciando conexion con la base de datos...")
            cnx = connect_ddbb()

        # Opcion de salida
        elif opcion == "9":
            break
        else:
            print("Opción no válida. Por favor, selecciona una opción válida.")


if __name__ == '__main__':
    columnas_eliminadas = ["infant_age_recode_22","130_infant_cause_recode","method_of_disposition", "autopsy", "icd_code_10th_revision", "number_of_entity_axis_conditions", "entity_condition_1", "entity_condition_2", "entity_condition_3", "entity_condition_4", "entity_condition_5", "entity_condition_6", "entity_condition_7", "entity_condition_8", "entity_condition_9", "entity_condition_10", "entity_condition_11", "entity_condition_12", "entity_condition_13", "entity_condition_14", "entity_condition_15", "entity_condition_16", "entity_condition_17", "entity_condition_18", "entity_condition_19", "entity_condition_20", "number_of_record_axis_conditions", "record_condition_1", "record_condition_2", "record_condition_3", "record_condition_4", "record_condition_5", "record_condition_6", "record_condition_7", "record_condition_8", "record_condition_9", "record_condition_10", "record_condition_11", "record_condition_12", "record_condition_13", "record_condition_14", "record_condition_15", "record_condition_16", "record_condition_17", "record_condition_18", "record_condition_19", "record_condition_20","age_recode_27","age_recode_12"]
    menu(columnas_eliminadas)

    
