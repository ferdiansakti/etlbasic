"""
Modul Transformasi Data Fashion

Membersihkan data hasil ekstraksi dengan aturan spesifik:
- Price: konversi ke rupiah
- Rating: float saja
- Colors: hanya angka
- Size & Gender: dibersihkan dari teks tambahan
"""

import pandas as pd
from datetime import datetime
import logging


# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def convert_price(price_str):
    """Konversi harga dari $ ke IDR (x16000)"""
    if isinstance(price_str, str) and price_str.startswith("$"):
        try:
            return float(price_str.replace("$", "")) * 16000
        except ValueError:
            return float('nan')
    return float('nan')


def clean_rating(rating_str):
    """Ekstrak rating sebagai float antara 0–5"""
    if isinstance(rating_str, str):
        if any(kata in rating_str.lower() for kata in ["invalid", "unknown"]):
            return float('nan')
        for part in rating_str.split():
            try:
                val = float(part)
                if 0 <= val <= 5:
                    return val
            except ValueError:
                continue
    return float('nan')


def extract_color_count(color_str):
    """Mengambil jumlah warna dari string seperti '3 Colors'"""
    if isinstance(color_str, str):
        for part in color_str.split():
            if part.isdigit():
                return int(part)
    return float('nan')


def clean_size(size_str):
    """Bersihkan kolom ukuran dari teks tambahan"""
    return size_str.replace("Size: ", "").strip() if isinstance(size_str, str) else ""


def clean_gender(gender_str):
    """Bersihkan kolom gender dari teks tambahan"""
    return gender_str.replace("Gender: ", "").strip() if isinstance(gender_str, str) else ""


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Melakukan transformasi data tanpa menyimpan ke file.

    Args:
        df (pd.DataFrame): Data mentah dari extract.

    Returns:
        pd.DataFrame: Data hasil transformasi.
    """
    logging.info("Memulai proses transformasi data...")

    # Validasi input
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input harus berupa DataFrame.")

    required_columns = ["Title", "Price", "Rating", "Colour", "Size", "Gender"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Kolom hilang: {missing_cols}")

    df = df.copy()

    # 1. Kolom Harga → konversi ke IDR
    df["Price"] = df["Price"].apply(convert_price)

    # 2. Kolom Rating → float valid
    df["Rating"] = df["Rating"].apply(clean_rating)

    # 3. Kolom Warna → hanya angka
    df["Colour"] = df["Colour"].apply(extract_color_count)

    # 4. Ukuran → bersihkan teks "Size: "
    df["Size"] = df["Size"].apply(clean_size).astype(str)

    # 5. Gender → bersihkan teks "Gender: "
    df["Gender"] = df["Gender"].apply(clean_gender).astype(str)

    # Filter baris yang memiliki nilai invalid pada kolom penting
    df = df.dropna(subset=["Price", "Rating", "Colour"]).copy()

    # Hapus duplikat dan reset index
    df = df.drop_duplicates().reset_index(drop=True)

    # Bersihkan nama kolom dari spasi berlebih
    df.columns = df.columns.str.strip()

    # Tambahkan timestamp
    df["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info("Transformasi berhasil.")
    return df


if __name__ == "__main__":
    INPUT_CSV = "data_scrapping.csv"

    print("Mulai proses transformasi data fashion...\n")
    try:
        df_raw = pd.read_csv(INPUT_CSV)
        print(f"Berhasil membaca {len(df_raw)} baris dari {INPUT_CSV}")

        df_clean = transform_data(df_raw)

        if not df_clean.empty:
            OUTPUT_CSV = "clean_data.csv"
            df_clean.to_csv(OUTPUT_CSV, index=False)
            print(f"Data berhasil dibersihkan dan disimpan ke {OUTPUT_CSV}")
            print("\nContoh data hasil:")
            print(df_clean.head())
        else:
            print("Tidak ada data yang dapat disimpan setelah transformasi.")

    except FileNotFoundError:
        print(f"File '{INPUT_CSV}' tidak ditemukan. Pastikan file tersedia.")
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")