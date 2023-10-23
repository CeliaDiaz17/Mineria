import json
import csv
import os
import pandas as pd 
import requests
import concurrent.futures
import threading
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
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

     
def leerDatosCsv(ruta):
    nRowsRead = None
    data = pd.read_csv(ruta, delimiter=',', nrows = nRowsRead,low_memory=False)
    data.dataframeName = ruta
    return data

def comprobaciones():
    folder_path="csv"
    cont=2005
    for filename in os.listdir(folder_path):
        total_rows_with_values=0
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            # Read the CSV file
            data = leerDatosCsv(file_path)
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
 
def suicideDataCsv(folder_path, columnasEliminadas):
    cont = 2005
    data = pd.DataFrame()
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            try:
                print("Inicio de la limpieza del año:", str(cont))
                dataTemp = (leerDatosCsv(file_path))

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

def unemploymentUSDataCsv(path):
    folder_path = path + "/unemployment_data_us.csv"
    data = leerDatosCsv(folder_path)
    data = data[(data['Year']>=2005) & (data['Year']<=2015)]
    shutil.rmtree(path)
    return data

def guardarCsv(dataframe, nombre_archivo):
    ruta = 'resultados/' + nombre_archivo
    dataframe.to_csv(ruta, index=False)

def descargarKaggle(download_dir,dataset_name):
    # Descarga con kaggle
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(str(dataset_name), path = str(download_dir), unzip = True)
    
def descargarSuicideRate(download_dir, url):
    # Use GeckoDriverManager to automatically download the compatible GeckoDriver for Firefox
    driver = webdriver.Firefox()
    driver.get(url)
    time.sleep(5)  # Give the page some time to load

    wait = WebDriverWait(driver, 10)  # You may adjust the timeout as needed

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
        time.sleep(10)

        downloaded_file = os.path.join(os.path.expanduser('~'), 'Downloads', 'data-table.csv').replace('\\', '/')
        destination_file = Path(os.path.join(download_dir, 'suicideRate.csv').replace('\\', '/'))

        # Ensure the destination directory exists
        os.makedirs(download_dir, exist_ok=True)

        # Move the file to the destination with the new name
        shutil.move(downloaded_file, destination_file)
        destination_file.chmod(0o644)

    driver.quit()


# https://www.cdc.gov/d2514623-b500-40e7-a472-c378514f86c3
def suicideRateDataCsv(folder_path, file_name):
    data = leerDatosCsv(folder_path+file_name)
    data = data[(data['YEAR']>=2005) & (data['YEAR']<=2015)]
    shutil.rmtree(folder_path)
    return data

def conectarBBDD():

    return 1

def menu():
    while True:
        opcion = input("Opciones:\n1. Descargar datos de mortalidad\n2. Procesar datos de SuicideRate\n3. Procesar datos de UnemploymentData\n4. Salir\nSelecciona una opción: ")

        if opcion == "1":
            download_dir = "csv/mortalidad/"
            dataset = "cdc/mortality"
            descargarKaggle(download_dir, dataset)
            print("Inicio de la unión de CSVs para generar SuicideData")
            data = suicideDataCsv(download_dir, columnasEliminadas)
            print("Inicio del guardado de datos...")
            guardarCsv(data, "suicideData.csv")
            print("Fin del guardado de datos en el archivo suicideData.csv")
        elif opcion == "2":
            download_dir = "csv/suicideRate/"
            dataset_url = "https://www.cdc.gov/nchs/pressroom/sosmap/suicide-mortality/suicide.htm"
            file_name = "suicideRate.csv"
            print("Inicio del procesamiento de SuicideRateData")
            descargarSuicideRate(download_dir, dataset_url)
            data = suicideRateDataCsv(download_dir, file_name)
            print("Inicio del guardado de datos...")
            guardarCsv(data, "suicideRateData.csv")
            print("Fin del guardado de datos en el archivo suicideRateData.csv")
        elif opcion == "3":
            print("Inicio del procesamiento de UnemploymentData")
            download_dir = "csv/unemployment"
            dataset = "aniruddhasshirahatti/us-unemployment-dataset-2010-2020"
            descargarKaggle(download_dir, dataset)
            data = unemploymentUSDataCsv(download_dir)
            print("Inicio del guardado de datos...")
            guardarCsv(data, "unemploymentUSData.csv")
            print("Fin del guardado de datos en el archivo unemploymentUSData.csv")
        elif opcion == "4":
            break
        else:
            print("Opción no válida. Por favor, selecciona una opción válida.")


if __name__ == '__main__':
    columnasEliminadas = ["infant_age_recode_22","130_infant_cause_recode","method_of_disposition", "autopsy", "icd_code_10th_revision", "number_of_entity_axis_conditions", "entity_condition_1", "entity_condition_2", "entity_condition_3", "entity_condition_4", "entity_condition_5", "entity_condition_6", "entity_condition_7", "entity_condition_8", "entity_condition_9", "entity_condition_10", "entity_condition_11", "entity_condition_12", "entity_condition_13", "entity_condition_14", "entity_condition_15", "entity_condition_16", "entity_condition_17", "entity_condition_18", "entity_condition_19", "entity_condition_20", "number_of_record_axis_conditions", "record_condition_1", "record_condition_2", "record_condition_3", "record_condition_4", "record_condition_5", "record_condition_6", "record_condition_7", "record_condition_8", "record_condition_9", "record_condition_10", "record_condition_11", "record_condition_12", "record_condition_13", "record_condition_14", "record_condition_15", "record_condition_16", "record_condition_17", "record_condition_18", "record_condition_19", "record_condition_20","age_recode_27","age_recode_12"]
    menu()

    
