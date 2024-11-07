from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import json
import logging

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

def fetch_data(**kwargs):
    """Fetch data from the API and save it as raw JSON."""
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

def transform_data(**kwargs):
    """Transform raw JSON data and save it as Parquet, partitioned by state."""
    try:
        # Load raw data from the Bronze layer
        df = pd.read_json(BRONZE_PATH)

        # Check for essential columns
        required_columns = ["id", "name", "brewery_type", "city", "state"]
        if not all(column in df.columns for column in required_columns):
            raise ValueError("Missing essential columns in the data")

        # Select necessary columns
        transformed_df = df[required_columns]

        # Convert to Parquet, partitioned by state
        table = pa.Table.from_pandas(transformed_df)
        pq.write_to_dataset(table, root_path=SILVER_PATH, partition_cols=["state"])
        logging.info("Silver layer data transformed and saved successfully, partitioned by state.")
    except Exception as e:
        logging.error(f"Error in transforming data: {e}")
        raise

def aggregate_data(**kwargs):
    """Aggregate transformed data and save it as Parquet."""
    try:
        # Load transformed data from the Silver layer
        df = pd.read_parquet(SILVER_PATH)

        # Aggregate data: count breweries by type and state
        aggregated_df = df.groupby(["brewery_type", "state"]).size().reset_index(name="brewery_count")

        # Save aggregated data as Parquet in the Gold layer
        aggregated_df.to_parquet(GOLD_PATH, index=False)
        logging.info("Gold layer data aggregated and saved successfully.")
    except Exception as e:
        logging.error(f"Error in aggregating data: {e}")
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

    # Bronze Layer Task - Fetch raw data
    fetch_task = PythonOperator(
        task_id='bronze_layer_fetch_data',
        python_callable=fetch_data,
        provide_context=True,
    )

    # Silver Layer Task - Transform data
    transform_task = PythonOperator(
        task_id='silver_layer_transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    # Gold Layer Task - Aggregate data
    aggregate_task = PythonOperator(
        task_id='gold_layer_aggregate_data',
        python_callable=aggregate_data,
        provide_context=True,
    )

    # Define execution sequence
    fetch_task >> transform_task >> aggregate_task
