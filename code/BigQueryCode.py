import os
from google.cloud import bigquery
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO

# Ubicación del archivo json con las credenciales de la cuenta de servicios
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keys/key.json"

# Inicializa el cliente de BigQuery
client = bigquery.Client()

def obtener_empresas_nasdaq100():
    url = 'https://en.wikipedia.org/wiki/NASDAQ-100'
    respuesta = requests.get(url)
    soup = BeautifulSoup(respuesta.text, 'html.parser')
    tabla = soup.find('table', {'id': 'constituents'})
    html_string = str(tabla)
    html_io = StringIO(html_string)
    df = pd.read_html(html_io)[0]
    return df['Symbol'].tolist()

def crear_dataset_si_no_existe(dataset_id):
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)  # Verificar si existe el dataset
        print(f"El Dataset {dataset_id} ya existe.")
    except Exception:  # Si no existe, lo crea
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f"El Dataset {dataset_id} fue creado.")

def guardar_data_en_BigQuery(ticker, dataset_id):
    data = yf.Ticker(ticker).history(period="max")
    if not data.empty:
        data = data.sort_values('Date')
        schema = [
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Open", "FLOAT"),
            bigquery.SchemaField("High", "FLOAT"),
            bigquery.SchemaField("Low", "FLOAT"),
            bigquery.SchemaField("Close", "FLOAT"),
            bigquery.SchemaField("Volume", "INTEGER"),
            bigquery.SchemaField("Dividends", "FLOAT"),
            bigquery.SchemaField("Stock Splits", "FLOAT"),
        ]
        table_id = f"{client.project}.{dataset_id}.{ticker}"
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = client.load_table_from_dataframe(data.reset_index(), table_id, job_config=job_config)
        job.result()  # Esperar hasta que se termine de subir
        print(f"Data para {ticker} fue subida a BigQuery.")

# Verificando si existe el dataset
dataset_id = 'tickers'
crear_dataset_si_no_existe(dataset_id)

# Obtener tickers y guardar data en BigQuery
tickers = obtener_empresas_nasdaq100()
for ticker in tickers:
    guardar_data_en_BigQuery(ticker, dataset_id)