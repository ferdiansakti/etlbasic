"""
Unit test untuk utils.transform.transform_data

Menguji apakah fungsi transform_data membersihkan dan memformat ulang data sesuai ekspektasi.
"""

import unittest
import pandas as pd
from datetime import datetime
import os
import sys

# Tambahkan path root proyek agar bisa import dari utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.transform import transform_data


class TestTransformData(unittest.TestCase):
    """Kelas unit test untuk fungsi transform_data()"""

    def setUp(self):
        """Buat DataFrame dummy untuk digunakan di semua test."""
        self.raw_data = pd.DataFrame({
            "Title": [
                "T-shirt 1",
                "Dress 2",
                "Pants 3",
                "Unknown Product",
                "Duplicate Item"
            ],
            "Price": [
                "$10.99",          # Valid
                "$unavailable",     # Invalid format
                "$invalid",         # Invalid format
                "$50.00",           # Akan dilewati karena produk 'Unknown'
                "$10.99"            # Duplikat harga & nama → akan dihapus
            ],
            "Rating": [
                "Rating: ⭐ 4.5 / 5",   # Valid
                "Rating: Not Rated",   # Invalid
                "Rating: ⭐ Invalid Rating / 5",  # Invalid
                "unknown rating",      # Invalid
                "Rating: ⭐ 4.5 / 5"    # Duplikat rating
            ],
            "Colour": [
                "3 Colors",        # Valid
                "Red Color",       # Tidak ada angka → invalid
                "Color Unavailable",  # Tidak ada angka → invalid
                "No Color",        # Invalid
                "3 Colors"         # Duplikat warna
            ],
            "Size": [
                "Size: S",
                "Size: XL",
                "Invalid Size",
                "",
                "Size: L"
            ],
            "Gender": [
                "Gender: Men",
                "Gender: Women",
                "Gender: Unisex",
                "Gender: Unknown",
                "Gender: Men"
            ]
        })

        # Dummy data dengan duplikat
        self.duplicate_data = pd.concat([self.raw_data, self.raw_data], ignore_index=True)

    def test_valid_data_transformation(self):
        """Test bahwa data valid diproses dengan benar."""
        result = transform_data(self.raw_data)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)  # Harus tersisa 2 baris setelah filtering
        self.assertTrue(all(col in result.columns for col in ["Title", "Price", "Rating"]))
        self.assertIn("Timestamp", result.columns)

    def test_invalid_input_returns_empty_df(self):
        """Test bahwa input bukan DataFrame menghasilkan error."""
        with self.assertRaises(ValueError):
            transform_data("bukan dataframe")

    def test_missing_columns_raises_error(self):
        """Test bahwa error terjadi jika kolom penting hilang."""
        df_missing = self.raw_data.drop(columns=["Title"])
        with self.assertRaises(KeyError):
            transform_data(df_missing)

    def test_price_conversion_to_idr(self):
        """Test konversi harga ke IDR."""
        result = transform_data(self.raw_data)
        expected_prices = [175840.0, 175840.0]  # $10.99 * 16000
        actual_prices = result["Price"].tolist()
        self.assertTrue(all(round(p, 2) in expected_prices for p in actual_prices))

    def test_rating_cleaning_and_filtering(self):
        """Test pembersihan dan filter kolom Rating."""
        result = transform_data(self.raw_data)
        valid_ratings = [4.5, 4.5]
        actual_ratings = result["Rating"].tolist()
        self.assertTrue(all(round(r, 1) in valid_ratings for r in actual_ratings))

    def test_color_extraction(self):
        """Test ekstraksi jumlah warna."""
        result = transform_data(self.raw_data)
        self.assertTrue(all(isinstance(c, int) or isinstance(c, float) for c in result["Colour"]))  # Float karena NaN handling
        self.assertEqual(set(result["Colour"]), {3})  # Hanya warna 3 yang valid

    def test_size_cleaning(self):
        """Test pembersihan kolom Ukuran."""
        result = transform_data(self.raw_data)
        self.assertTrue(all(item in ["S", "L"] for item in result["Size"]))

    def test_gender_cleaning(self):
        """Test pembersihan kolom Gender."""
        result = transform_data(self.raw_data)
        self.assertTrue(all(item in ["Men", "Women", "Unisex"] for item in result["Gender"]))

    def test_duplicate_removal(self):
        """Test penghapusan duplikasi."""
        result = transform_data(self.duplicate_data)
        self.assertEqual(len(result), 2)  # Masih 2 baris unik

    def test_null_values_removed(self):
        """Test bahwa baris dengan null dilewati."""
        result = transform_data(self.raw_data)
        self.assertFalse(result.isnull().values.any())

    def test_unavailable_products_filtered(self):
        """Test produk dengan field invalid harus dihapus."""
        result = transform_data(self.raw_data)
        product_names = result["Title"].unique()
        self.assertNotIn("Unknown Product", product_names)

    def test_adds_timestamp_column(self):
        """Test kolom Timestamp ditambahkan dengan benar."""
        result = transform_data(self.raw_data)
        self.assertIn("Timestamp", result.columns)
        try:
            pd.to_datetime(result["Timestamp"])
        except Exception:
            self.fail("Kolom 'Timestamp' tidak dalam format datetime yang valid")

    def test_all_required_columns_present(self):
        """Test semua kolom penting tetap ada setelah transformasi."""
        result = transform_data(self.raw_data)
        required_cols = ["Title", "Price", "Rating", "Colour", "Size", "Gender", "Timestamp"]
        self.assertTrue(all(col in result.columns for col in required_cols))

    def test_non_numeric_colors_are_handled(self):
        """Test warna non-angka dilewati."""
        no_color_data = self.raw_data.copy()
        no_color_data.loc[0, "Colour"] = "Red|Blue|Green"

        result = transform_data(no_color_data)
        self.assertEqual(len(result[result["Colour"].notna()]), 1)

    def test_invalid_rows_are_skipped(self):
        """Test baris dengan nilai invalid benar-benar dihapus."""
        result = transform_data(self.raw_data)
        self.assertEqual(len(result), 2)

    def test_transform_handles_empty_dataframe(self):
        """Test bahwa transform_data mengembalikan DataFrame kosong jika input kosong."""
        empty_df = pd.DataFrame(columns=self.raw_data.columns)
        result = transform_data(empty_df)
        self.assertTrue(result.empty)

    def test_transform_with_partial_missing_data(self):
        """Test baris dengan beberapa field kosong dilewati."""
        partial_missing = self.raw_data.copy()
        partial_missing.loc[0, "Price"] = "$invalid"
        partial_missing.loc[1, "Rating"] = ""

        result = transform_data(partial_missing)
        self.assertEqual(len(result), 1)  # Harusnya hanya satu baris valid

    def test_output_dtypes(self):
        """Test tipe data hasil transformasi sesuai ekspektasi."""
        result = transform_data(self.raw_data)
        self.assertEqual(result["Price"].dtype, float)
        self.assertEqual(result["Rating"].dtype, float)
        self.assertEqual(result["Colour"].dtype, float)  # Karena NaN jadi float
        self.assertEqual(result["Size"].dtype, object)
        self.assertEqual(result["Gender"].dtype, object)

    def test_transform_with_mixed_case_columns(self):
        """Test bahwa kolom berbeda kapitalisasi tidak diterima."""
        mixed_case_data = self.raw_data.copy()
        mixed_case_data.columns = ["TITLE", "PRICE", "RATING", "COLOUR", "SIZE", "GENDER"]
        with self.assertRaises(KeyError):
            transform_data(mixed_case_data)

    def test_transform_with_extra_columns(self):
        """Test bahwa kolom tambahan tidak mengganggu proses."""
        extra_data = self.raw_data.copy()
        extra_data["Extra Column"] = "value"
        result = transform_data(extra_data)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn("Timestamp", result.columns)


if __name__ == "__main__":
    unittest.main()