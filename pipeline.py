import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def baixar_arquivos(url_pasta, diretorio_local):
  os.makedirs(diretorio_local, exist_ok=True)
  gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)
  
def listar_arquivos_csv(diretorio):
  arquivos_csv = []
  todos_arquivos = os.listdir(diretorio)
  for arquivo in todos_arquivos:
    if arquivo.endswith(".csv"):
      caminho_completo = os.path.join(diretorio, arquivo)
      arquivos_csv.append(caminho_completo)
    print(arquivos_csv)
    return arquivos_csv

def ler_csv(caminho_do_arquivo):
  dataframe = duckdb.read_csv(caminho_do_arquivo)
  print(dataframe)
  return dataframe

def transformar(df):
  df_transformado = duckdb.sql("""
    SELECT 
        "data",
        AVG("temperatura-mininima") AS media_minima,
        AVG("temperatura-maxima") AS media_maxima,
        (AVG("temperatura-mininima") + AVG("temperatura-maxima")) / 2 AS coluna_teste
    FROM df
    GROUP BY "data"
    """).df()
  print(df_transformado)
  return df_transformado

def salvar_no_postegres(df_duckdb, tabela):
  DATABASE_URL os.getenv("DATABASE_URL")
  engine = create_engine(DATABASE_URL)
  #Salvar o Dataframe no postgres
  df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)

if __name__ == "__main__":
  url_pasta = 'https://drive.google.com/drive/folders/1FZ6DIoz7kM3diE_mXzHHodmfuXLkPzra'
  diretorio_local = './pasta_gdown'
  #baixar_arquivos(url_pasta, diretorio_local)
  arquivo = listar_arquivos_csv(diretorio_local)
  dataframe_duckdb = ler_csv(arquivo)
  transformar(dataframe_duckdb)