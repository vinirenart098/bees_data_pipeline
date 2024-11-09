import unittest
from unittest.mock import patch
import pandas as pd
import os
import json
import sys
import logging

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
        # Get the directory of this script to save the log in the same folder
        script_dir = os.path.dirname(__file__)
        log_path = os.path.join(script_dir, "test_log.txt")

        # Logger setup for logging test results to a file in the same directory
        cls.logger = logging.getLogger("TestDataPipeline")
        cls.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(log_path, mode="w")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        cls.logger.addHandler(file_handler)
        cls.logger.addHandler(logging.StreamHandler())

    def log_info(self, message):
        # Helper method to log messages
        self.logger.info(message)

    @patch('requests.get')
    def test_fetch_data_creates_bronze_file(self, mock_get):
        # Test that BronzeLayer fetches data and creates bronze file
        self.log_info("Starting test: test_fetch_data_creates_bronze_file")
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {"id": 1, "name": "Brewery", "brewery_type": "micro", "city": "City", "state": "Oklahoma"}
        ]

        BronzeLayer.fetch_data()
        
        self.assertTrue(os.path.exists(BRONZE_FILE_PATH), "Bronze data file was not created.")
        with open(BRONZE_FILE_PATH, "r") as file:
            data = json.load(file)
            self.assertEqual(data[0]["state"], "Oklahoma", "State data in Bronze layer does not match expected value.")
        self.log_info("test_fetch_data_creates_bronze_file completed successfully.")

    def test_validate_bronze_data_format(self):
        # Test that BronzeLayer validates data format correctly
        self.log_info("Starting test: test_validate_bronze_data_format")
        BronzeLayer.validate_data()
        self.log_info("test_validate_bronze_data_format completed successfully.")

    def test_transform_data_creates_silver_files(self):
        # Test that SilverLayer transforms data and creates silver files for each state
        self.log_info("Starting test: test_transform_data_creates_silver_files")
        SilverLayer.transform_data()
        self.assertTrue(os.path.exists(SILVER_DIR_PATH), "Silver layer directory does not exist.")
        state_files = [f for f in os.listdir(SILVER_DIR_PATH) if f.endswith(".parquet")]
        self.assertTrue(len(state_files) > 0, "No Parquet files found in the Silver layer directory.")
        self.log_info("test_transform_data_creates_silver_files completed successfully.")

    def test_validate_silver_data_by_state(self):
        # Test that SilverLayer validates each state file correctly
        self.log_info("Starting test: test_validate_silver_data_by_state")
        SilverLayer.validate_data()
        self.log_info("test_validate_silver_data_by_state completed successfully.")

    def test_aggregate_data_creates_gold_file(self):
        # Test that GoldLayer aggregates data and creates gold file
        self.log_info("Starting test: test_aggregate_data_creates_gold_file")
        GoldLayer.aggregate_data()
        self.assertTrue(os.path.exists(GOLD_FILE_PATH), "Gold data file was not created.")
        self.log_info("test_aggregate_data_creates_gold_file completed successfully.")

    def test_validate_gold_data_structure(self):
        # Test that GoldLayer validates the structure of the aggregated data
        self.log_info("Starting test: test_validate_gold_data_structure")
        GoldLayer.validate_data()
        self.log_info("test_validate_gold_data_structure completed successfully.")

if __name__ == '__main__':
    unittest.main()
