"""
Unit test untuk utils/load.py

Menguji apakah fungsi load_data():
- Menyimpan ke CSV
- Upload ke Google Sheets (mocked)
- Simpan ke PostgreSQL (mocked)
"""

import unittest
import os
import sys
import pandas as pd
from unittest.mock import patch, MagicMock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.load import load_data

class TestLoadData(unittest.TestCase):
    def setUp(self):
        self.test_df = pd.DataFrame({
            "Title": ["T-shirt 1", "Dress 2"],
            "Price": [175840.0, 320000.0],
            "Rating": [4.5, 3.8],
            "Colour": [3, 5],
            "Size": ["S", "L"],
            "Gender": ["Men", "Women"]
        })

        self.db_url = "postgresql://user:pass@localhost:5432/dbname"
        self.csv_output = "test_products.csv"  # Diubah untuk konsistensi dengan load.py

        # Bersihkan file output sebelumnya jika ada
        if os.path.exists(self.csv_output):
            os.remove(self.csv_output)

    def tearDown(self):
        # Bersihkan file output setelah test
        if os.path.exists(self.csv_output):
            os.remove(self.csv_output)

    @patch('utils.load.save_to_csv')
    @patch('utils.load.upload_to_google_sheets')
    @patch('utils.load.save_to_postgres')
    def test_valid_data_loading(self, mock_postgres, mock_gsheet, mock_csv):
        """Test bahwa semua metode load berhasil dengan data valid."""
        # Setup mock return values
        mock_csv.return_value = True
        mock_gsheet.return_value = True
        mock_postgres.return_value = True

        result = load_data(self.test_df, self.db_url, self.csv_output)
        
        # Verifikasi semua fungsi dipanggil
        mock_csv.assert_called_once()
        mock_gsheet.assert_called_once()
        mock_postgres.assert_called_once()
        
        # Verifikasi hasil
        self.assertTrue(result["csv"])
        self.assertTrue(result["google_sheets"])
        self.assertTrue(result["postgresql"])

    def test_empty_dataframe_handling(self):
        """Test bahwa tidak ada error jika input DataFrame kosong."""
        empty_df = pd.DataFrame(columns=self.test_df.columns)
        
        with patch('utils.load.save_to_csv') as mock_csv, \
             patch('utils.load.upload_to_google_sheets') as mock_gsheet, \
             patch('utils.load.save_to_postgres') as mock_postgres:
            
            mock_csv.return_value = False
            mock_gsheet.return_value = False
            mock_postgres.return_value = False
            
            result = load_data(empty_df, self.db_url, self.csv_output)
            
            self.assertFalse(result["csv"])
            self.assertFalse(result["google_sheets"])
            self.assertFalse(result["postgresql"])

    def test_missing_required_columns(self):
        """Test bahwa error terjadi jika kolom penting hilang."""
        df_missing = self.test_df.drop(columns=["Price"])
        with self.assertRaises(KeyError):
            load_data(df_missing, self.db_url)

    def test_invalid_input_type(self):
        """Test bahwa error terjadi jika input bukan DataFrame."""
        with self.assertRaises(ValueError):
            load_data("bukan dataframe", self.db_url)

    @patch('utils.load.save_to_csv', return_value=True)
    @patch('utils.load.upload_to_google_sheets', side_effect=Exception("Google Sheets gagal"))
    @patch('utils.load.save_to_postgres', return_value=True)
    def test_partial_failure_handled(self, mock_postgres, mock_gsheet, mock_csv):
        """Test bahwa error satu bagian tidak menghentikan proses lain."""
        result = load_data(self.test_df, self.db_url, self.csv_output)
        self.assertTrue(result["csv"])
        self.assertFalse(result["google_sheets"])
        self.assertTrue(result["postgresql"])

    @patch('utils.load.save_to_csv', side_effect=Exception("Simulasi kesalahan CSV"))
    @patch('utils.load.upload_to_google_sheets', return_value=True)
    @patch('utils.load.save_to_postgres', return_value=True)
    def test_csv_failure_handled(self, mock_postgres, mock_gsheet, mock_csv):
        """Test bahwa error CSV ditangani dengan baik."""
        result = load_data(self.test_df, self.db_url, self.csv_output)
        self.assertFalse(result["csv"])
        self.assertTrue(result["google_sheets"])
        self.assertTrue(result["postgresql"])

    @patch('utils.load.save_to_csv', return_value=True)
    @patch('utils.load.upload_to_google_sheets', return_value=True)
    @patch('utils.load.save_to_postgres', return_value=False)
    def test_postgres_failure_handled(self, mock_postgres, mock_gsheet, mock_csv):
        """Test bahwa error PostgreSQL tetap lanjut ke tujuan lain."""
        result = load_data(self.test_df, self.db_url, self.csv_output)
        self.assertTrue(result["csv"])
        self.assertTrue(result["google_sheets"])
        self.assertFalse(result["postgresql"])

    @patch('utils.load.save_to_csv', return_value=True)
    @patch('utils.load.upload_to_google_sheets', return_value=False)
    @patch('utils.load.save_to_postgres', return_value=True)
    def test_gsheet_failure_handled(self, mock_postgres, mock_gsheet, mock_csv):
        """Test bahwa error Google Sheets tetap lanjut ke tujuan lain."""
        result = load_data(self.test_df, self.db_url, self.csv_output)
        self.assertTrue(result["csv"])
        self.assertFalse(result["google_sheets"])
        self.assertTrue(result["postgresql"])

if __name__ == "__main__":
    unittest.main()