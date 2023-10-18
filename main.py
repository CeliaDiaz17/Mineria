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
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService 
from webdriver_manager.chrome import ChromeDriverManager 
     
#"/datasets/cdc/mortality/download?datasetVersionNumber=2" 
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
    return data

def suicideRateDataCsv(path):
    data = leerDatosCsv(path)
    data = data[(data['YEAR']>=2005) & (data['YEAR']<=2015)]
    return data

def unemploymentUSDataCsv(path):
    data = leerDatosCsv(path)
    data = data.drop('Date', axis = 1, inplace = True)
    data = data[(data['Year']>=2005) & (data['Year']<=2015)]
    return data

def guardarCsv(dataframe, nombre_archivo):
    ruta = 'resultados/' + nombre_archivo
    dataframe.to_csv(ruta, index=False)

def descargaCsvSuicidiosFederal(url, output_path):
    try:
        # Download the file
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Create the output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        # Determine the output file path
        output_file = os.path.join(output_path, "archive.zip")

        # Save the file
        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # Try to extract the contents of the ZIP file
        try:
            with zipfile.ZipFile(output_file, 'r') as zip_ref:
                zip_ref.extractall(output_path)
        except zipfile.BadZipFile:
            # If it's not a ZIP file, identify the file type
            with magic.Magic() as magic_obj:
                file_type = magic_obj.from_file(output_file)
                print(f"The downloaded file is not a valid ZIP archive. It is of type: {file_type}")

        # Delete the downloaded file
        os.remove(output_file)

        print(f"File downloaded and extracted to {output_path}")
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def descragarKaggle(download_dir,dataset_name):
    kaggle.api.authenticate(api_key="path_to_your_kaggle.json")
    try:
        # Create the download directory if it doesn't exist
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        # Download the dataset
        kaggle.api.dataset_download_files(dataset_name, path=download_dir, unzip=True)
        return True  # Download successful
    except Exception as e:
        print(f"Error: {e}")
        return False  # Download failed


def descargaCsvTasaSuicidioEstatal(url):
    # Inicializa un navegador web controlado por Selenium
    options = webdriver.ChromeOptions()
    options.headless = True
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)  # Asegúrate de tener ChromeDriver instalado y en tu PATH
    # Abre la página web en el navegador controlado por Selenium
    driver.get(url)
    elements=driver.find_elements(By.CLASS_NAME, 'theme-blue no-border')
    print(driver.page_source)
    

def descargaCsvTasaDesempleo(url):
    r =requests.get(url=url)
    soup = BeautifulSoup(r.content, "html.parser")

    print(soup)

    


if __name__ == '__main__':
    columnasEliminadas = ["infant_age_recode_22","130_infant_cause_recode","method_of_disposition", "autopsy", "icd_code_10th_revision", "number_of_entity_axis_conditions", "entity_condition_1", "entity_condition_2", "entity_condition_3", "entity_condition_4", "entity_condition_5", "entity_condition_6", "entity_condition_7", "entity_condition_8", "entity_condition_9", "entity_condition_10", "entity_condition_11", "entity_condition_12", "entity_condition_13", "entity_condition_14", "entity_condition_15", "entity_condition_16", "entity_condition_17", "entity_condition_18", "entity_condition_19", "entity_condition_20", "number_of_record_axis_conditions", "record_condition_1", "record_condition_2", "record_condition_3", "record_condition_4", "record_condition_5", "record_condition_6", "record_condition_7", "record_condition_8", "record_condition_9", "record_condition_10", "record_condition_11", "record_condition_12", "record_condition_13", "record_condition_14", "record_condition_15", "record_condition_16", "record_condition_17", "record_condition_18", "record_condition_19", "record_condition_20","age_recode_27","age_recode_12"]

    # Descarga death data
    #descargaCsvSuicidiosFederal("https://www.kaggle.com/datasets/cdc/mortality/download?datasetVersionNumber=2", "C:/Users/garci/proyectos/practicasMineriaDatos/csv/SuicideData")
    descragarKaggle("C:/Users/garci/proyectos/practicasMineriaDatos/csv/SuicideData","cdc/mortality")
    #Crear data csv's
    """ print("Inicio de la union de csv's para generar SuicideData")
    data = suicideDataCsv("C:/Users/garci/proyectos/practicasMineriaDatos/csv/SuicideData",columnasEliminadas)
    print('Inicio del guardado de datos...')
    guardarCsv(data,'suicideData.csv')
    print('Fin del guardado de datos en el archivo suicideData.csv') """

    """ print('Inicio del procesamiento de SuicideRateData')
    data = suicideRateDataCsv("C:/Users/garci/proyectos/practicasMineriaDatos/csv/SuicideRateData/data-table.csv")
    print('Inicio del guardado de datos...')
    guardarCsv(data, 'suicideRateData')
    print('Fin del guardado de datos en el archivo suicideRateData.csv') """

    """ print('Inicio del procesamiento de UnemploymentData')
    data = unemploymentUSDataCsv("C:/Users/garci/proyectos/practicasMineriaDatos/csv/unemploymentData/unemployment_data_us.csv")
    print('Inicio del guardado de datos...')
    guardarCsv(data, 'unemploymentUSData')
    print('Fin del guardado de datos en el archivo unemploymentUSData.csv')   """  


    """ print("Inicio de la union de csv's para generar SuicideData")
    data = suicideDataCsvConcurrente("C:/Users/garci/proyectos/practicasMineriaDatos/csv/SuicideData", columnasEliminadas)
    print('Inicio del guardado de datos...')
    guardarCsv(data,'suicideDataConcu.csv')
    print('Fin del guardado de datos en el archivo suicideDataConcu.csv') """


    #Dynamic webScrapping
    #descargaCsvSuicidiosFederal('https://www.kaggle.com/datasets/cdc/mortality/data?select=2015_codes.json')
    #descargaCsvTasaSuicidioEstatal("https://www.cdc.gov/nchs/pressroom/sosmap/suicide-mortality/suicide.htm")
    #descargaCsvTasaDesempleo("https://www.kaggle.com/datasets/aniruddhasshirahatti/us-unemployment-dataset-2010-2020")

    #Comprobaciones
    #comprobaciones2('resultados/data.csv',None)
    #california("2021-05-14_deaths_final_1999_2013_state_year_sup.csv")
    #comprobaciones()
    """ data=leerDatosCsv("csv/2005_data.csv")
    print(data[(data['manner_of_death']==2) & (data['130_infant_cause_recode'].notnull())]) """
    
