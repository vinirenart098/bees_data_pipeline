import unittest
from unittest.mock import patch
import pandas as pd
import os
import json
import sys
import logging
import hashlib

# Add the root directory to sys.path for relative imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from root.dags.brewery_dag import BronzeLayer, SilverLayer, GoldLayer

# Define relative paths for test data
DATA_DIR = os.path.join("root", "dags", "data")
BRONZE_FILE_PATH = os.path.join(DATA_DIR, "bronze_breweries.json")
SILVER_DIR_PATH = os.path.join(DATA_DIR, "silver_breweries")
GOLD_FILE_PATH = os.path.join(DATA_DIR, "gold_breweries_aggregated.parquet")


class TestDataPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Setup logger for test results
        script_dir = os.path.dirname(__file__)
        log_path = os.path.join(script_dir, "test_log.txt")
        
        cls.logger = logging.getLogger("TestDataPipeline")
        cls.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(log_path, mode="w")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        cls.logger.addHandler(file_handler)
        cls.logger.addHandler(logging.StreamHandler())

    def log_info(self, message):
        # Helper method to log messages
        self.logger.info(message)

    @staticmethod
    def calculate_file_hash(filepath):
        # Calculate MD5 hash for a file to check for idempotency
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @patch('requests.get')
    def test_fetch_data_creates_bronze_file(self, mock_get):
        self.log_info("Starting test: test_fetch_data_creates_bronze_file")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {"id": 1, "name": "Brewery", "brewery_type": "micro", "city": "City", "state": "Oklahoma"}
        ]

        BronzeLayer.fetch_data()
        
        # Check if Bronze file was created and validate its contents
        self.assertTrue(os.path.exists(BRONZE_FILE_PATH), "Bronze data file was not created.")
        with open(BRONZE_FILE_PATH, "r") as file:
            data = json.load(file)
            self.assertEqual(data[0]["state"], "Oklahoma", "State data in Bronze layer does not match expected value.")
            required_columns = ["id", "name", "brewery_type", "city", "state"]
            for record in data:
                self.assertTrue(all(column in record for column in required_columns), "Missing columns in Bronze data")
        self.log_info("test_fetch_data_creates_bronze_file completed successfully.")

    def test_validate_bronze_data_format(self):
        self.log_info("Starting test: test_validate_bronze_data_format")
        BronzeLayer.validate_data()
        self.log_info("test_validate_bronze_data_format completed successfully.")

    def test_transform_data_creates_silver_files(self):
        self.log_info("Starting test: test_transform_data_creates_silver_files")
        SilverLayer.transform_data()
        
        # Check if Silver directory exists and if state files were created
        self.assertTrue(os.path.exists(SILVER_DIR_PATH), "Silver layer directory does not exist.")
        state_files = [f for f in os.listdir(SILVER_DIR_PATH) if f.endswith(".parquet")]
        self.assertTrue(len(state_files) > 0, "No Parquet files found in the Silver layer directory.")
        
        # Check structure and idempotency of Silver files
        for state_file in state_files:
            file_path = os.path.join(SILVER_DIR_PATH, state_file)
            df = pd.read_parquet(file_path)
            
            # Validate essential columns and data integrity
            required_columns = ["id", "name", "brewery_type", "city", "state"]
            self.assertTrue(all(col in df.columns for col in required_columns), f"Missing columns in {state_file}")
            self.assertFalse(df[required_columns].isnull().any().any(), f"Null values found in {state_file}")

            # Validate that each file contains data for the correct state
            state_name = state_file.replace(".parquet", "")
            self.assertTrue(df['state'].nunique() == 1 and df['state'].iloc[0] == state_name,
                            f"Incorrect state data in {state_file}")

            # Optional: calculate hash to ensure idempotency
            file_hash = self.calculate_file_hash(file_path)
            self.log_info(f"Hash for {state_file}: {file_hash}")
        
        self.log_info("test_transform_data_creates_silver_files completed successfully with data integrity checks.")

    def test_validate_silver_data_by_state(self):
        self.log_info("Starting test: test_validate_silver_data_by_state")
        SilverLayer.validate_data()
        self.log_info("test_validate_silver_data_by_state completed successfully.")

    def test_aggregate_data_creates_gold_file(self):
        self.log_info("Starting test: test_aggregate_data_creates_gold_file")
        GoldLayer.aggregate_data()

        # Check if Gold file was created
        self.assertTrue(os.path.exists(GOLD_FILE_PATH), "Gold data file was not created.")
        
        # Load and validate structure and aggregated data
        df = pd.read_parquet(GOLD_FILE_PATH)
        self.assertIn("brewery_type", df.columns, "brewery_type column missing in Gold layer data")
        self.assertIn("state", df.columns, "state column missing in Gold layer data")
        self.assertIn("brewery_count", df.columns, "brewery_count column missing in Gold layer data")
        self.assertFalse(df["brewery_count"].isnull().any(), "brewery_count contains null values in Gold layer data")

        self.log_info("test_aggregate_data_creates_gold_file completed successfully.")

    def test_validate_gold_data_structure(self):
        self.log_info("Starting test: test_validate_gold_data_structure")
        GoldLayer.validate_data()
        self.log_info("test_validate_gold_data_structure completed successfully.")

if __name__ == '__main__':
    unittest.main()
