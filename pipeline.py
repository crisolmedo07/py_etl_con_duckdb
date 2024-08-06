import os
import gdown
import duckdb
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv


def bajar_archivos_google_drive(url_gd , directorio_local):
    os.makedirs(directorio_local , exist_ok=True)
    gdown.download_folder(url_gd , output = directorio_local , quiet = False , use_cookies = False)


if __name__ == "__main__":
    url_gd = "https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP"
    directorio_local = "./file_down"
    bajar_archivos_google_drive(url_gd , directorio_local)

