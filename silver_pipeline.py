import requests
import pdfplumber
import pandas as pd
import re
from io import BytesIO
from datetime import datetime
import os

# Archivo CSV donde guardaremos el histórico
CSV_FILE = "silver_oi.csv"

def get_silver_oi():
    url = "https://www.cmegroup.com/daily_bulletin/current/Section02B_Summary_Volume_And_Open_Interest_Metals_Futures_And_Options.pdf"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("No se pudo descargar el PDF")
        return None

    text = ""
    with pdfplumber.open(BytesIO(response.content)) as pdf:
        for page in pdf.pages:
            text += page.extract_text() + "\n"

    lines = text.split("\n")
    silver_line = None
    for line in lines:
        if "SI COMEX SILVER FUTURES" in line.upper():
            silver_line = line
            break
    if not silver_line:
        print("No se encontró SI COMEX SILVER FUTURES")
        return None

    numbers = re.findall(r"\d[\d,]*", silver_line)
    if len(numbers) < 4:
        print("Formato inesperado")
        return None

    overall_volume = int(numbers[2].replace(",", ""))
    open_interest = int(numbers[3].replace(",", ""))

    # Fecha del reporte en el PDF
    datematch = re.search(r"[A-Za-z]+ \d{1,2}, \d{4}", text)
    if not datematch:
        print("No se encontró la fecha en el PDF")
        return None
    report_date = datetime.strptime(datematch.group(), "%b %d, %Y").date()

    # Crear DataFrame de este día
    df = pd.DataFrame({
        "date": [report_date],
        "volume": [overall_volume],
        "open_interest": [open_interest]
    })

    return df

def update_csv():
    df_new = get_silver_oi()
    if df_new is None:
        return

    # Si no existe CSV, lo creamos con encabezados
    if not os.path.exists(CSV_FILE):
        df_new.to_csv(CSV_FILE, index=False)
    else:
        # Append sin duplicar fechas
        df_existing = pd.read_csv(CSV_FILE)
        if df_new["date"].iloc[0] not in df_existing["date"].values:
            df_new.to_csv(CSV_FILE, mode="a", index=False, header=False)

    print(f"Datos guardados en {CSV_FILE}")

if __name__ == "__main__":
    update_csv()
