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
ALERT_FILE_PATH = "/opt/airflow/dags/data/validation_alerts.csv"
API_URL = "https://api.openbrewerydb.org/breweries"
TIMEOUT = 10  # API request timeout in seconds
RETRY_COUNT = 3  # Number of retries for failed tasks
RETRY_DELAY = timedelta(minutes=5)  # Delay between retries
EXECUTION_TIMEOUT = timedelta(minutes=10)  # Maximum execution time for each task

# Logging configuration
logging.basicConfig(level=logging.INFO)

# Utility function to ensure a directory exists
def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created missing directory: {directory}")

# Bronze Layer
class BronzeLayer:
    @staticmethod
    def fetch_data():
        # Ensure that the directory for Bronze data exists
        ensure_directory_exists(BRONZE_PATH)

        try:
            response = requests.get(API_URL, timeout=TIMEOUT)
            response.raise_for_status()  # Raise an error if the response is not successful
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
        """Validates Bronze layer data for required fields, unique IDs, and logs fields with null values."""
        try:
            df = pd.read_json(BRONZE_PATH)
            required_columns = ["id", "name", "brewery_type", "city", "state", "address_1"]
            alerts = []

            # Check if all required columns are present
            if not all(column in df.columns for column in required_columns):
                missing_columns = [col for col in required_columns if col not in df.columns]
                raise ValueError(f"Bronze data is missing required columns: {missing_columns}")

            # Identify rows with any null values and log details for each missing field
            for index, row in df.iterrows():
                null_fields = row[row.isnull()].index.tolist()
                if null_fields:
                    logging.warning(
                        f"Record with ID {row['id']} has missing values in the following fields: {', '.join(null_fields)}. "
                        f"Details: Name={row['name']}, City={row['city']}, State={row['state']}"
                    )
                    alerts.append({
                        "id": row["id"],
                        "name": row["name"],
                        "missing_fields": ", ".join(null_fields),
                        "city": row["city"],
                        "state": row["state"],
                        "brewery_type": row["brewery_type"],
                        "postal_code": row.get("postal_code", "N/A"),
                        "website_url": row.get("website_url", "N/A"),
                        "phone": row.get("phone", "N/A")
                    })

            # Identify duplicate IDs
            duplicate_ids = df[df.duplicated("id", keep=False)]
            if not duplicate_ids.empty:
                for index, row in duplicate_ids.iterrows():
                    logging.warning(
                        f"Duplicate 'id' found: {row['id']} in {row['name']} located in {row['city']}, {row['state']}. "
                        f"Details: Brewery Type={row['brewery_type']}, Postal Code={row.get('postal_code', 'N/A')}, Website={row.get('website_url', 'N/A')}"
                    )
                    alerts.append({
                        "id": row["id"],
                        "name": row["name"],
                        "issue": "Duplicate ID",
                        "city": row["city"],
                        "state": row["state"],
                        "brewery_type": row["brewery_type"],
                        "postal_code": row.get("postal_code", "N/A"),
                        "website_url": row.get("website_url", "N/A"),
                        "phone": row.get("phone", "N/A")
                    })

            # If there are any alerts, save them to a CSV file for review
            if alerts:
                ensure_directory_exists(ALERT_FILE_PATH)
                pd.DataFrame(alerts).to_csv(ALERT_FILE_PATH, index=False)
                logging.info(f"Alerts saved to {ALERT_FILE_PATH}")

            logging.info("Bronze layer validation passed.")
        except Exception as e:
            logging.error(f"Error validating Bronze layer data: {e}")
            raise

# Silver Layer
class SilverLayer:
    @staticmethod
    def transform_data():
        """Transforms Bronze data and saves it as Parquet files, one per state."""
        # Make sure the directory for Silver data exists
        ensure_directory_exists(SILVER_PATH)

        try:
            df = pd.read_json(BRONZE_PATH)
            required_columns = ["id", "name", "brewery_type", "city", "state", "address_1"]
            if not all(column in df.columns for column in required_columns):
                raise ValueError("Missing essential columns in the data")

            # Select only required columns and split data by state
            transformed_df = df[required_columns]
            states = transformed_df['state'].unique()
            for state in states:
                state_df = transformed_df[transformed_df['state'] == state]
                parquet_path = os.path.join(SILVER_PATH, f"{state}.parquet")
                
                # Ensure the Silver subdirectory for each state exists
                ensure_directory_exists(parquet_path)
                
                # Save each state's data as a Parquet file
                table = pa.Table.from_pandas(state_df)
                pq.write_table(table, parquet_path)
                logging.info(f"Silver layer data for state '{state}' saved successfully as Parquet.")
        except Exception as e:
            logging.error(f"Error in transforming data: {e}")
            raise

    @staticmethod
    def validate_data():
        """Validates that each Silver file contains data for only one state and logs fields with null values."""
        try:
            alerts = []
            for file in os.listdir(SILVER_PATH):
                if file.endswith(".parquet"):
                    state_name = file.replace(".parquet", "")
                    df = pd.read_parquet(os.path.join(SILVER_PATH, file))
                    
                    # Ensure data in each file is consistent with the state it represents
                    if df['state'].nunique() != 1 or df['state'].iloc[0] != state_name:
                        logging.warning(f"Inconsistent state data in {file}")
                        alerts.append(f"Inconsistent state data in {file}")
                    
                    # Check for missing values and log the fields with null values
                    for index, row in df.iterrows():
                        null_fields = row[row.isnull()].index.tolist()
                        if null_fields:
                            logging.warning(f"Record {row['id']} in state '{state_name}' has null values in fields: {', '.join(null_fields)}")
                            alerts.append({
                                "id": row["id"],
                                "name": row["name"],
                                "missing_fields": ", ".join(null_fields),
                                "state": row["state"],
                                "brewery_type": row["brewery_type"],
                                "postal_code": row.get("postal_code", "N/A"),
                                "website_url": row.get("website_url", "N/A"),
                                "phone": row.get("phone", "N/A")
                            })

            # If there are alerts, save them to a CSV file
            if alerts:
                ensure_directory_exists(ALERT_FILE_PATH)
                pd.DataFrame(alerts).to_csv(ALERT_FILE_PATH, index=False)
                logging.info(f"Silver layer alerts saved to {ALERT_FILE_PATH}")

            logging.info("Silver layer validation passed.")
        except Exception as e:
            logging.error(f"Error validating Silver layer data: {e}")
            raise

# Gold Layer
class GoldLayer:
    @staticmethod
    def aggregate_data():
        """Aggregates Silver data and saves it as a single Parquet file in the Gold layer."""
        # Ensure the Gold data directory exists
        ensure_directory_exists(GOLD_PATH)

        try:
            parquet_files = [os.path.join(SILVER_PATH, f) for f in os.listdir(SILVER_PATH) if f.endswith('.parquet')]
            dataframes = [pd.read_parquet(file) for file in parquet_files]
            df = pd.concat(dataframes, ignore_index=True)

            # Confirm essential columns for aggregation
            if 'brewery_type' not in df.columns or 'state' not in df.columns:
                raise ValueError("Missing essential columns in Silver layer data for aggregation.")

            # Group by brewery type and state, count the number of breweries, and save to Parquet
            aggregated_df = df.groupby(["brewery_type", "state"]).size().reset_index(name="brewery_count")
            aggregated_df.to_parquet(GOLD_PATH, index=False)
            logging.info("Gold layer data aggregated and saved successfully.")
        except Exception as e:
            logging.error(f"Error in aggregating data: {e}")
            raise

    @staticmethod
    def validate_data():
        """Checks Gold data for the expected structure and values."""
        try:
            df = pd.read_parquet(GOLD_PATH)
            # Ensure 'brewery_count' exists and has no null values
            if 'brewery_count' not in df.columns or df['brewery_count'].isnull().any():
                raise ValueError("Gold data validation failed: 'brewery_count' column is missing or contains nulls.")
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

    # Define the execution sequence with validation tasks
    fetch_task >> bronze_validation_task >> transform_task >> silver_validation_task >> aggregate_task >> gold_validation_task
