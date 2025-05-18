import logging
import pandas as pd
from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from datetime import datetime


class FashionSpider(Spider):
    """Spider untuk mengambil data fashion dari halaman web"""

    name = "fashion_spider"
    start_urls = ["https://fashion-studio.dicoding.dev/"]

    custom_settings = {
        'USER_AGENT': 'FashionDataCollectorBot/1.0',
        'LOG_LEVEL': 'INFO',
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'data_scrapping.csv',  # Menyimpan langsung dalam format file CSV
        'CLOSESPIDER_ITEMCOUNT': 1000,     # Tutup setelah 1000 item (sesuai dengan rubrik penilaian)
    }

    def parse(self, response):
        """Mengekstrak informasi produk dari halaman web"""
        for card in response.css("div.collection-card"):
            yield {
                "Title": card.css("h3.product-title::text").get(),
                "Price": card.css("span.price::text").get(),
                "Rating": card.css("div.product-details > p:nth-child(3)::text").get(),
                "Colour": card.css("div.product-details > p:nth-child(4)::text").get(),
                "Size": card.css("div.product-details > p:nth-child(5)::text").get(),
                "Gender": card.css("div.product-details > p:nth-child(6)::text").get(),
            }

        # Melakukan pengecekan apakah ada halaman selanjutnya
        next_page = response.css("li.page-item.next > a.page-link::attr(href)").get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield Request(url=next_page_url, callback=self.parse)


def eksekusi_pengambilan_data():
    """
    Fungsi utama untuk menjalankan proses pengambilan data
    """
    try:
        process = CrawlerProcess()
        process.crawl(FashionSpider)
        process.start()

        df = pd.read_csv("data_scrapping.csv")
        return df

    except Exception as e:
        logging.error(f"Terjadi kesalahan saat menjalankan spider: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    print("Memulai proses pengumpulan data fashion...")
    hasil_df = eksekusi_pengambilan_data()

    if not hasil_df.empty:
        print("Data berhasil dikumpulkan dan disimpan di 'data_scrapping.csv'")
        print("\nContoh data:")
        print(hasil_df.head())
    else:
        print("Tidak ada data yang berhasil dikumpulkan.")
