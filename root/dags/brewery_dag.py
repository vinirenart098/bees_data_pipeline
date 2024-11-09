from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import json
import logging
import os

# Configuration values for the DAG
BRONZE_PATH = "/opt/airflow/dags/data/bronze_breweries.json"
SILVER_PATH = "/opt/airflow/dags/data/silver_breweries"
GOLD_PATH = "/opt/airflow/dags/data/gold_breweries_aggregated.parquet"
API_URL = "https://api.openbrewerydb.org/breweries"
TIMEOUT = 10  # API request timeout in seconds
RETRY_COUNT = 3  # Number of retries for failed tasks
RETRY_DELAY = timedelta(minutes=5)  # Delay between retries
EXECUTION_TIMEOUT = timedelta(minutes=10)  # Maximum execution time for each task

# Logging configuration
logging.basicConfig(level=logging.INFO)

# Bronze Layer
class BronzeLayer:
    @staticmethod
    def fetch_data():
        try:
            response = requests.get(API_URL, timeout=TIMEOUT)
            response.raise_for_status()  # Raise an error for bad responses
            breweries = response.json()

            # Save raw data to JSON in the Bronze layer
            with open(BRONZE_PATH, "w") as file:
                json.dump(breweries, file)
            logging.info("Bronze layer data fetched and saved successfully.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            raise  # Re-raise the exception for Airflow to handle retries

    @staticmethod
    def validate_data():
        """Check if Bronze layer data has the required fields and no nulls."""
        try:
            df = pd.read_json(BRONZE_PATH)
            required_columns = ["id", "name", "brewery_type", "city", "state"]
            if not all(column in df.columns for column in required_columns):
                raise ValueError("Bronze data is missing required columns")
            if df[required_columns].isnull().any().any():
                raise ValueError("Bronze data contains null values")
            logging.info("Bronze layer validation passed.")
        except Exception as e:
            logging.error(f"Error validating Bronze layer data: {e}")
            raise

# Silver Layer
class SilverLayer:
    @staticmethod
    def transform_data():
        """Transform raw JSON data and save it as separate Parquet files for each state."""
        try:
            if not os.path.exists(SILVER_PATH):
                os.makedirs(SILVER_PATH)
                logging.info(f"Silver layer directory created at {SILVER_PATH}")

            df = pd.read_json(BRONZE_PATH)
            required_columns = ["id", "name", "brewery_type", "city", "state"]
            if not all(column in df.columns for column in required_columns):
                raise ValueError("Missing essential columns in the data")

            transformed_df = df[required_columns]
            states = transformed_df['state'].unique()
            for state in states:
                state_df = transformed_df[transformed_df['state'] == state]
                parquet_path = os.path.join(SILVER_PATH, f"{state}.parquet")
                table = pa.Table.from_pandas(state_df)
                pq.write_table(table, parquet_path)
                logging.info(f"Silver layer data for state '{state}' saved successfully as Parquet.")
        except Exception as e:
            logging.error(f"Error in transforming data: {e}")
            raise

    @staticmethod
    def validate_data():
        """Check if each Silver layer file contains only the specified state's data."""
        try:
            for file in os.listdir(SILVER_PATH):
                if file.endswith(".parquet"):
                    state_name = file.replace(".parquet", "")
                    df = pd.read_parquet(os.path.join(SILVER_PATH, file))
                    if df['state'].nunique() != 1 or df['state'].iloc[0] != state_name:
                        raise ValueError(f"Silver layer validation failed for state {state_name}")
            logging.info("Silver layer validation passed.")
        except Exception as e:
            logging.error(f"Error validating Silver layer data: {e}")
            raise

# Gold Layer
class GoldLayer:
    @staticmethod
    def aggregate_data():
        """Aggregate transformed data and save it as Parquet."""
        try:
            parquet_files = [os.path.join(SILVER_PATH, f) for f in os.listdir(SILVER_PATH) if f.endswith('.parquet')]
            dataframes = [pd.read_parquet(file) for file in parquet_files]
            df = pd.concat(dataframes, ignore_index=True)

            if 'brewery_type' not in df.columns or 'state' not in df.columns:
                raise ValueError("Missing essential columns in Silver layer data for aggregation.")

            aggregated_df = df.groupby(["brewery_type", "state"]).size().reset_index(name="brewery_count")
            aggregated_df.to_parquet(GOLD_PATH, index=False)
            logging.info("Gold layer data aggregated and saved successfully.")
        except Exception as e:
            logging.error(f"Error in aggregating data: {e}")
            raise

    @staticmethod
    def validate_data():
        """Ensure Gold layer data contains expected structure and values."""
        try:
            df = pd.read_parquet(GOLD_PATH)
            if 'brewery_count' not in df.columns or df['brewery_count'].isnull().any():
                raise ValueError("Gold data validation failed: brewery_count column missing or contains nulls.")
            logging.info("Gold layer validation passed.")
        except Exception as e:
            logging.error(f"Error validating Gold layer data: {e}")
            raise

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': RETRY_COUNT,
    'retry_delay': RETRY_DELAY,
    'execution_timeout': EXECUTION_TIMEOUT,
}

with DAG(
    'brewery_data_pipeline_medallion',
    default_args=default_args,
    description='A data pipeline for brewery data using Medallion Architecture',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Bronze Layer Tasks
    fetch_task = PythonOperator(
        task_id='bronze_layer_fetch_data',
        python_callable=BronzeLayer.fetch_data,
    )

    bronze_validation_task = PythonOperator(
        task_id='bronze_layer_validate_data',
        python_callable=BronzeLayer.validate_data,
    )

    # Silver Layer Tasks
    transform_task = PythonOperator(
        task_id='silver_layer_transform_data',
        python_callable=SilverLayer.transform_data,
    )

    silver_validation_task = PythonOperator(
        task_id='silver_layer_validate_data',
        python_callable=SilverLayer.validate_data,
    )

    # Gold Layer Tasks
    aggregate_task = PythonOperator(
        task_id='gold_layer_aggregate_data',
        python_callable=GoldLayer.aggregate_data,
    )

    gold_validation_task = PythonOperator(
        task_id='gold_layer_validate_data',
        python_callable=GoldLayer.validate_data,
    )

    # Define execution sequence with validation tasks
    fetch_task >> bronze_validation_task >> transform_task >> silver_validation_task >> aggregate_task >> gold_validation_task
