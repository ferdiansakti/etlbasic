"""
ETL Pipeline for Fashion Data

Main script that orchestrates the complete ETL process:
1. Extract data from fashion website
2. Transform the raw data
3. Load the cleaned data to multiple destinations
"""

import logging
from utils.extract import eksekusi_pengambilan_data
from utils.transform import transform_data
from utils.load import load_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuration constants
RAW_DATA_CSV = "data_scrapping.csv"
CLEAN_DATA_CSV = "clean_data.csv"
DB_URL = "postgresql://postgres:123456@localhost:5432/savetosql"

def run_etl_pipeline():
    """
    Execute the complete ETL pipeline:
    1. Extract data from source
    2. Transform the raw data
    3. Load to multiple destinations
    """
    try:
        logging.info("Starting ETL pipeline...")
        
        # EXTRACT PHASE
        logging.info("Phase 1: Extracting data...")
        raw_df = eksekusi_pengambilan_data()
        
        if raw_df.empty:
            logging.error("Extraction failed - no data collected")
            return
        
        # Save raw data for reference
        raw_df.to_csv(RAW_DATA_CSV, index=False)
        logging.info(f"Raw data saved to {RAW_DATA_CSV}")
        
        # TRANSFORM PHASE
        logging.info("Phase 2: Transforming data...")
        clean_df = transform_data(raw_df)
        
        if clean_df.empty:
            logging.error("Transformation failed - no valid data after cleaning")
            return
            
        # LOAD PHASE
        logging.info("Phase 3: Loading data...")
        load_data(
            df=clean_df,
            db_url=DB_URL,
            csv_output=CLEAN_DATA_CSV
        )
        
        logging.info("ETL pipeline completed successfully!")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_etl_pipeline()