import yfinance as yf  # librería para descargar con yahoo finance
import pandas as pd
import datetime as dt
import sqlite3

# Diccionario con las empresas de las que se traen los datos
dict_Empresas = {
    "Apple": "AAPL",
    "Microsoft": "MSFT",
    "Meta": "META",
    "Tesla": "TSLA",
    # Algunas sugerencias:
    # "Alphabet": "GOOGL",
    # "Amazon": "AMZN",
    # "JPMorgan": "JPM",
    # "Johnson & Johnson": "JNJ",
    # "Walmart": "WMT"
}


def load_data(dict_Empresas, start=dt.date(2023, 1, 1), end=dt.date.today()):
    data_list = []
    for key, value in dict_Empresas.items():
        # Se descargan los datos desde 01/01/23 hasta hoy
        Dataset = yf.download(value, start=start, end=end)

        # Edito Nombres Columnas
        Dataset.columns = ["Close", "High", "Low", "Open", "Volume"]
        Dataset = Dataset.reset_index()

        # Agrego Nombre de empresa
        Dataset["Empresa"] = key
        Dataset["TKT"] = value

        data_list.append(Dataset)

        # Se imprime en pantalla un mensaje de confirmacion
        print(f"Archivo {key} cargado exitosamente.")

    return pd.concat(data_list)


def write_sql_main(DB, df, table):
    """Esta funcion inserta masivamente un df o una lista de tuplas en SQL."""
    try:
        conn = sqlite3.connect(DB)
        print("Connected to SQLite")
        df.to_sql(
            table,
            conn,
            schema=None,
            if_exists="replace",
            index=False,
            index_label=None,
            chunksize=None,
            dtype=None,
            method=None,
        )
        result = "ok"
    except sqlite3.Error as error:
        print("Failed to insert multiple records into sqlite table", error)
        result = "failed"
    finally:
        if conn:
            conn.close()
            print("The SQLite connection is closed")
    return result
