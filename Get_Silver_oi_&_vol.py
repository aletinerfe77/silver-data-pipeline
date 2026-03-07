import requests
import pdfplumber
import pandas as pd
import re
from io import BytesIO
from datetime import datetime
import sqlite3
import yfinance as yf

DB_FILE = "silver_data.db"


# -------------------------------------------------
# CREAR TABLA SI NO EXISTE
# -------------------------------------------------

def init_db():

    conn = sqlite3.connect(DB_FILE)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS silver_market_data (
        date TEXT PRIMARY KEY,
        price REAL,
        volume INTEGER,
        open_interest INTEGER
    )
    """)

    conn.close()


# -------------------------------------------------
# EXTRAER OPEN INTEREST CME
# -------------------------------------------------

def get_cme_data():

    url = "https://www.cmegroup.com/daily_bulletin/current/Section02B_Summary_Volume_And_Open_Interest_Metals_Futures_And_Options.pdf"

    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception("No se pudo descargar el PDF")

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
        raise Exception("No se encontró SI COMEX SILVER FUTURES")

    numbers = re.findall(r"\d[\d,]*", silver_line)

    volume = int(numbers[2].replace(",", ""))
    open_interest = int(numbers[3].replace(",", ""))

    datematch = re.search(r"[A-Za-z]+ \d{1,2}, \d{4}", text)

    report_date = datetime.strptime(datematch.group(), "%b %d, %Y").date()

    return report_date, volume, open_interest


# -------------------------------------------------
# EXTRAER PRECIO
# -------------------------------------------------

def get_price():

    data = yf.download(
        "SI=F",
        period="5d",
        interval="1d",
        progress=False,
        auto_adjust=True
    )

    today = pd.Timestamp.today().normalize()

    data = data[data.index < today]

    price = data["Close"].iloc[-1].values[0]
    price_date = data.index[-1].date()

    return price_date, price


# -------------------------------------------------
# GUARDAR EN BASE DE DATOS
# -------------------------------------------------

def save_data(date, price, volume, open_interest):

    conn = sqlite3.connect(DB_FILE)

    conn.execute("""
    INSERT OR REPLACE INTO silver_market_data
    (date, price, volume, open_interest)
    VALUES (?, ?, ?, ?)
    """, (date, price, volume, open_interest))

    conn.commit()
    conn.close()


# -------------------------------------------------
# PIPELINE PRINCIPAL
# -------------------------------------------------

def run_pipeline():

    init_db()

    cme_date, volume, oi = get_cme_data()

    price_date, price = get_price()

    # usamos la fecha del CME (oficial)
    save_data(cme_date, price, volume, oi)

    print("Datos guardados:")
    print(f"Fecha: {cme_date}")
    print(f"Precio: {price}")
    print(f"Volumen: {volume}")
    print(f"Open Interest: {oi}")


# -------------------------------------------------

run_pipeline()