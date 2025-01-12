import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

url_pasta = ''
diretorio_local = './pasta_gdown'

def baixar_arquivos(url_pasta, diretorio_local):
  os.makedirs(diretorio_local, exist_ok=True)
  gdown.dowmload_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)