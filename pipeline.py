import os
import gdown
import duckdb
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

load_dotenv() # cargar variable de ambiente

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
    #print(dataframe_duckdb)
    #print(type(dataframe_duckdb))
    return dataframe_duckdb

# Funcion para adicionar una columna de total de ventas como ejemplo
def transformar_dataframe(df: DuckDBPyRelation) -> DataFrame: # tipaje de entrada y salida del objeto
    # Ejecuta la consulta SQL que incluye una nueva columna, operando sobre una tabla virtual
    df_transformado = duckdb.sql( "select * , quantidade * valor as total_vendas from df").df()
    # Remueve el regitro de la tabla virtual
    return df_transformado

# Funcion para convertir Duckdb em Pandas y guardar el dataframe en PostgreSQL
def guardar_en_postgres(df_duckdb , tabla):
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    # guardar el df en el banco
    df_duckdb.to_sql(tabla , con=engine , if_exists='append' , index=False)


# funcion principal
if __name__ == "__main__":
    url_gd = "https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP"
    dir_local = "./file_down"
    #bajar_archivos_google_drive(url_gd , dir_local)
    lista_archivo = listar_archivos_csv(dir_local)

    for dir_archivo in lista_archivo:
        dataframe_duckdb = leer_csv(dir_archivo)
        pandas_df_transformado = transformar_dataframe(dataframe_duckdb)
        guardar_en_postgres(pandas_df_transformado , 'vendas_calculado')

