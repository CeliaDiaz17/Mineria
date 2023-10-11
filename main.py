import json
import csv
import os
import pandas as pd 
     
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

def comprobaciones2(ruta_csv=None, dataFrame=None):
    if ruta_csv is not None:
        data=leerDatosCsv(ruta_csv)
    if dataFrame is not None:
        data=dataFrame
    
    suicide=data[(data['manner_of_death']==2)]
    injury = data[(data['manner_of_death']==2) & (data['injury_at_work']=='Y')]
    activity=data[(data['manner_of_death']==2) & (data['activity_code'].notnull())]
    cause=data[(data['manner_of_death']==2) & ((data['358_cause_recode']) | (data['113_cause_recode']) | (data['39_cause_recode']))]
    allCause=data[(data['358_cause_recode'].notnull()) & (data['113_cause_recode'].notnull()) & (data['39_cause_recode'].notnull())]
    infantSuicide=data[(data['manner_of_death']==2) & (data['130_infant_cause_recode'].notnull())]
    minus19=data[(data['age_recode_52']<30) & (data['manner_of_death']==2)]
    noEducationRegisterSucide=data[((data['education']==99) | (data['education']==9)) & (data['manner_of_death']==2)]
    # Print data
    numSuicide=len(suicide)
    print(f"\tNumber of rows with suicide and 'injury_at_work': {len(injury)} percentage: {len(injury)/numSuicide*100}%")
    print(f"\tNumber of rows with suicide and 'activity_code': {len(activity)} percentage: {len(activity)/numSuicide*100}%")
    print(f"\tNumber of rows with suicide and 'cause_recode': {len(cause)} percentage: {len(cause)/numSuicide*100}%")
    print(f"\tNumber of rows with all 'cause_recode': {len(allCause)} percentage: {len(allCause)/len(data)*100}%")
    print(f"\tNumber of rows of infant suicide: {len(infantSuicide)} percentage: {len(infantSuicide)/numSuicide*100}%")
    print(f"\tNumber of less than 19 years old suicide: {len(minus19)} percentage: {len(minus19)/numSuicide*100}%")
    print(f"\tNumber of suicided ppl with no education register: {len(noEducationRegisterSucide)} percentage: {len(noEducationRegisterSucide)/numSuicide*100}%")
    print(f"\tTotal rows: {len(data)}")
 
def archivosCsv(folder_path, columnasEliminadas):
    cont = 2005
    data = pd.DataFrame()
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            try:
                dataTemp = (leerDatosCsv(file_path))
                columnasEl = [col for col in columnasEliminadas if col in dataTemp.columns.to_list()]
                dataTemp.drop(columnasEl, axis = 1, inplace = True)
                # Define un diccionario de mapeo para realizar la sustitución de valores
                mapeo_valores = {1: 8, 2: 19, 3: 20, 4: 21, 5: 22, 6: 23, 7: 24, 8: 25, 9: 26}
                # Utiliza la función 'replace' para aplicar el mapeo a la columna 'education_2003_revision'
                dataTemp['education_2003_revision'] = dataTemp['education_2003_revision'].replace(mapeo_valores)
                dataTemp['education'] = dataTemp['education_1989_revision'].fillna(dataTemp['education_2003_revision'])
                dataTemp.drop(['education_1989_revision', 'education_2003_revision'], axis=1, inplace=True)
                data = pd.concat([data, dataTemp], ignore_index=True)
            except pd.errors.EmptyDataError:
                print(f"El archivo {filename} está vacío.")
            print(cont)
            cont+=1
    return data

def guardarCsv(dataframe, nombre_archivo):
    dataframe.to_csv(nombre_archivo, index=False)

#mongodb+srv://<username>:<password>@datamineria.tnu4fw1.mongodb.net/?retryWrites=true&w=majority
def cargarDatosMongo(database_name, mongo_uri, username, password, dataFrame=None, folder_path=None, collection_name="datos"):
    try:
        client = MongoClient(mongo_uri)
        # Seleccionar la base de datos
        db = client[database_name]  
        # Autenticar con el usuario y contraseña
        db.authenticate(username, password)
        if dataFrame is not None:
            data = dataFrame
        elif folder_path is not None:
            # Leer el archivo CSV en un DataFrame de pandas
            data = pd.read_csv(folder_path)
        else:
            raise ValueError("Debes proporcionar un DataFrame o una ruta de carpeta válida.")
        # Convertir el DataFrame a una lista de diccionarios
        data_dict_list = data.to_dict(orient='records')            
        # Seleccionar la colección
        collection = db[collection_name]           
        # Insertar los datos en la colección
        collection.insert_many(data_dict_list)       
        print("Datos cargados exitosamente en MongoDB.")        
    except Exception as e:
        print(f"Error al cargar datos en MongoDB: {str(e)}")
    finally:
        client.close()
    


if __name__ == '__main__':
    columnasEliminadas = ["infant_age_recode_22","130_infant_cause_recode","method_of_disposition", "autopsy", "icd_code_10th_revision", "number_of_entity_axis_conditions", "entity_condition_1", "entity_condition_2", "entity_condition_3", "entity_condition_4", "entity_condition_5", "entity_condition_6", "entity_condition_7", "entity_condition_8", "entity_condition_9", "entity_condition_10", "entity_condition_11", "entity_condition_12", "entity_condition_13", "entity_condition_14", "entity_condition_15", "entity_condition_16", "entity_condition_17", "entity_condition_18", "entity_condition_19", "entity_condition_20", "number_of_record_axis_conditions", "record_condition_1", "record_condition_2", "record_condition_3", "record_condition_4", "record_condition_5", "record_condition_6", "record_condition_7", "record_condition_8", "record_condition_9", "record_condition_10", "record_condition_11", "record_condition_12", "record_condition_13", "record_condition_14", "record_condition_15", "record_condition_16", "record_condition_17", "record_condition_18", "record_condition_19", "record_condition_20","age_recode_27","age_recode_12"]
    
    #Crear data csv
    """ data = archivosCsv("csv",columnasEliminadas)
    print('Inicio del guardado de datos...')
    guardarCsv(data,'resultados/data.csv')
    print('Fin del guardado de datos') """

    

    #comprobaciones2('resultados/data.csv')
    #california("2021-05-14_deaths_final_1999_2013_state_year_sup.csv")
    #comprobaciones()
    """ data=leerDatosCsv("csv/2005_data.csv")
    print(data[(data['manner_of_death']==2) & (data['130_infant_cause_recode'].notnull())]) """
    
