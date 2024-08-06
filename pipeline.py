import os
import gdown
import duckdb
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv

# descargar archivos del google drive, con una biblioteca no oficial a modo de ejemplo
def bajar_archivos_google_drive(url_gd , directorio_local):
    os.makedirs(directorio_local , exist_ok=True)
    gdown.download_folder(url_gd , output = directorio_local , quiet = False , use_cookies = False)

# listar archivos de solo de csv
def listar_archivos_csv(directorio):
    archivo_csv =[]
    todos_los_archivos = os.listdir(directorio)
    for archivo in todos_los_archivos:
        if archivo.endswith(".csv"):
            dir_completo = os.path.join(directorio , archivo)
            archivo_csv.append(dir_completo) 
    return archivo_csv

# Funcion para leer el archivo CSV e retornar un tipo de Dataframe duckdb
def leer_csv(dir_archivo):
    dataframe_duckdb = duckdb.read_csv(dir_archivo)
    print(dataframe_duckdb)
    print(type(dataframe_duckdb))
    return dataframe_duckdb

# funcion principal
if __name__ == "__main__":
    url_gd = "https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP"
    dir_local = "./file_down"
    #bajar_archivos_google_drive(url_gd , dir_local)
    archivos = listar_archivos_csv(dir_local)
    leer_csv(archivos)

