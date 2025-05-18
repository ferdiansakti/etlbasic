"""
Modul Load Data

Menyimpan data hasil transformasi ke:
- CSV (flat file)
- Google Sheets
- PostgreSQL
"""

import os
import logging
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Konfigurasi Google Sheets
SERVICE_ACCOUNT_FILE = "censdored.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets "]
SPREADSHEET_ID = "censored"
SHEET_RANGE = "Sheet1!A1:G1000"


def save_to_csv(df: pd.DataFrame, output_file: str):
    """
    Menyimpan DataFrame ke file CSV.
    """
    try:
        output_path = os.path.join(os.path.dirname(__file__), "..", output_file)
        df.to_csv(output_path, index=False)
        logging.info(f"Data berhasil disimpan ke CSV: {output_path}")
        return True
    except Exception as e:
        logging.error(f"Gagal menyimpan ke CSV: {e}")
        return False


def upload_to_google_sheets(df: pd.DataFrame):
    """
    Mengunggah DataFrame ke Google Sheets.
    """
    try:
        credential = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build("sheets", "v4", credentials=credential)
        sheet = service.spreadsheets()

        values = [df.columns.tolist()] + df.astype(str).values.tolist()
        body = {"values": values}

        result = (
            sheet.values()
            .update(
                spreadsheetId=SPREADSHEET_ID,
                range=SHEET_RANGE,
                valueInputOption="RAW",
                body=body
            )
            .execute()
        )

        updated_cells = result.get('updatedCells', 0)
        logging.info(f"Berhasil mengunggah ke Google Sheets. {updated_cells} sel diperbarui.")
        return True
    except FileNotFoundError:
        logging.error("File kredensial Google tidak ditemukan.")
        return False
    except HttpError as e:
        logging.error(f"Kesalahan Google Sheets API: {e}")
        return False
    except Exception as e:
        logging.error(f"Gagal mengunggah ke Google Sheets: {e}")
        return False


def save_to_postgres(df: pd.DataFrame, db_url: str, table_name: str = "fashion_products"):
    """
    Menyimpan DataFrame ke database PostgreSQL.
    """
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            df.to_sql(table_name, con=conn, if_exists="replace", index=False)
        logging.info(f"Data berhasil disimpan ke PostgreSQL ({table_name})")
        return True
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
        return False
    except Exception as e:
        logging.error(f"Gagal menyimpan ke PostgreSQL: {e}")
        return False


def load_data(df: pd.DataFrame, db_url: str, csv_output: str = "products.csv"):  # Diubah ke products.csv
    """
    Memuat data ke semua destinasi yang tersedia.

    Returns:
        dict: Status tiap operasi (csv, google_sheets, postgresql)
    """
    logging.info("Mulai proses pemuatan data...")

    # Validasi input
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input harus berupa DataFrame.")

    required_columns = ["Title", "Price", "Rating", "Colour", "Size", "Gender"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Kolom hilang: {missing_cols}")

    results = {}

    # Simpan ke CSV
    try:
        results["csv"] = save_to_csv(df, csv_output)
    except Exception as e:
        logging.error(f"CSV Error: {e}")
        results["csv"] = False

    # Upload ke Google Sheets
    try:
        results["google_sheets"] = upload_to_google_sheets(df)
    except Exception as e:
        logging.error(f"Google Sheets Error: {e}")
        results["google_sheets"] = False

    # Simpan ke PostgreSQL
    try:
        results["postgresql"] = save_to_postgres(df, db_url)
    except Exception as e:
        logging.error(f"PostgreSQL Error: {e}")
        results["postgresql"] = False

    logging.info("Proses pemuatan data selesai.")
    return results


if __name__ == "__main__":
    INPUT_CSV = "clean_data.csv"  # ← Masih menggunakan clean_data.csv karena ini file input
    input_path = os.path.join(os.path.dirname(__file__), "..", INPUT_CSV)

    try:
        df_test = pd.read_csv(input_path)
    except FileNotFoundError:
        logging.error(f"File {input_path} tidak ditemukan.")
        exit(1)

    DB_URL = "postgresql://postgres:123456@localhost:5432/savetosql"

    print("Memulai proses load data...")
    result = load_data(df_test, DB_URL)  # Baris ini akan menyimpan ke products.csv

    print("\nRingkasan hasil:")
    for key, val in result.items():
        status = "Berhasil" if val else "Gagal"
        print(f"{key}: {status}")