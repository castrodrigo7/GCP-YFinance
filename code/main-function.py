import os
import yfinance as yf
from google.cloud import bigquery
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

client = bigquery.Client()

def obtener_lista_tickers():
    url = 'https://en.wikipedia.org/wiki/NASDAQ-100'
    try:
        respuesta = requests.get(url)
        soup = BeautifulSoup(respuesta.text, 'html.parser')
        tabla = soup.find('table', {'id': 'constituents'})
        if tabla:
            df = pd.read_html(str(tabla))[0]
            return df['Symbol'].tolist()
    except Exception as e:
        print(f"Error al obtener datos de Wikipedia: {e}")
    return []

def obtener_fechas_existentes(ticker):
    try:
        table_id = f"{client.project}.tickers.nasdaq100_historical_data"
        query = f"""
        SELECT Date FROM `{table_id}`
        WHERE Symbol = '{ticker}'
        """
        query_job = client.query(query)
        fechas_existentes = query_job.result()
        return set(row.Date for row in fechas_existentes)
    except Exception as e:
        print(f"Error al obtener fechas existentes para {ticker}: {e}")
        return set()

def procesar_ticker(ticker):
    try:
        data = yf.Ticker(ticker).history(period="1d")
        if not data.empty:
            data['Date'] = pd.to_datetime(data.index).date  
            data['Dividends'] = data.get('Dividends', 0.0)
            data['Stock Splits'] = data.get('Stock Splits', 0.0)
            data['Symbol'] = ticker

            fechas_existentes = obtener_fechas_existentes(ticker)
            return data[~data['Date'].isin(fechas_existentes)]
    except Exception as e:
        print(f"Error al actualizar {ticker}: {e}")
    return pd.DataFrame()

def actualizar_datos(request):
    dataset_id = 'tickers'
    tickers = obtener_lista_tickers()

    if not tickers:
        return "No se pudo obtener la lista de tickers del NASDAQ-100", 500

    # Ejecuta en paralelo el procesamiento de cada ticker
    all_data = pd.DataFrame()
    with ThreadPoolExecutor(max_workers=10) as executor:
        resultados = executor.map(procesar_ticker, tickers)
        for data in resultados:
            if not data.empty:
                all_data = pd.concat([all_data, data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Symbol']]])

    if not all_data.empty:
        table_id = f"{client.project}.{dataset_id}.nasdaq100_historical_data"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("Date", "DATE"), 
                bigquery.SchemaField("Open", "FLOAT"),
                bigquery.SchemaField("High", "FLOAT"),
                bigquery.SchemaField("Low", "FLOAT"),
                bigquery.SchemaField("Close", "FLOAT"),
                bigquery.SchemaField("Volume", "INTEGER"),
                bigquery.SchemaField("Dividends", "FLOAT"),
                bigquery.SchemaField("Stock Splits", "FLOAT"),
                bigquery.SchemaField("Symbol", "STRING"),
            ]
        )

        try:
            client.load_table_from_dataframe(all_data, table_id, job_config=job_config).result()
            print("Datos actualizados en BigQuery.")
        except Exception as e:
            print(f"Error al cargar los datos en BigQuery: {e}")
            return f"Error al cargar los datos en BigQuery: {e}", 500
    else:
        print("No hay nuevos datos para actualizar.")
        return "No hay nuevos datos para actualizar.", 200

    return "Actualización completa", 200