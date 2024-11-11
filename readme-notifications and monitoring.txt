
# 📊 Monitoring and Alerting System for the Data Pipeline

This document provides guidelines on setting up a monitoring and alerting system for the data pipeline using Apache Airflow. The focus is on ensuring data quality, detecting pipeline failures, and notifying the team about critical issues.

---

## 1. Data Quality Monitoring

To ensure data meets quality standards before processing, validation checks are implemented at different stages of the pipeline. Here are the main methods:

### A. Bronze Layer - Schema and Completeness Validation
- **Class**: `BronzeLayer`
- **Method**: `validate_data()`
- **Description**: This method checks if the data in the Bronze layer contains the required columns (`id`, `name`, `brewery_type`, `city`, `state`) and ensures there are no null values.
- **Location in DAG**: `bronze_layer_validate_data` task in the DAG (`brewery_data_pipeline_medallion`) within `brewery_dag.py`.
- **Unit Test Reference**: The `test_validate_bronze_data_format` function in `test_pipeline.py` verifies the format and completeness of the Bronze layer data.

### B. Silver Layer - Data Partition and Consistency Check
- **Class**: `SilverLayer`
- **Method**: `validate_data()`
- **Description**: This method verifies that each file in the Silver layer contains data only for the specified state, ensuring the partitioning by state is accurate.
- **Location in DAG**: `silver_layer_validate_data` task in the DAG (`brewery_data_pipeline_medallion`) within `brewery_dag.py`.
- **Unit Test Reference**: The `test_validate_silver_data_by_state` function in `test_pipeline.py` ensures each state's data is properly partitioned and consistent in the Silver layer.

### C. Gold Layer - Aggregation Validation
- **Class**: `GoldLayer`
- **Method**: `validate_data()`
- **Description**: This method validates the structure of the aggregated data in the Gold layer, checking for the presence of the `brewery_count` column and ensuring it contains no null values.
- **Location in DAG**: `gold_layer_validate_data` task in the DAG (`brewery_data_pipeline_medallion`) within `brewery_dag.py`.
- **Unit Test Reference**: The `test_validate_gold_data_structure` function in `test_pipeline.py` validates the aggregation structure and integrity in the Gold layer.

### Complementary Validation - Notebooks
In addition to automated validations, a Jupyter notebook (`visualizer.ipynb`) in the `notebooks/` directory allows for manual inspection of data across Bronze and Silver layers:
- **Function `validate_state_data`**: Verifies that the data for a selected state in the Bronze layer matches the data in the Silver layer for the same state.
- **Function `validate_all_states_in_gold_layer`**: Ensures that data in the Silver layer for all states is consistent with the aggregation in the Gold layer.

### Unit Tests
- **File**: `test_pipeline.py` in the `scripts/` folder includes unit tests that validate each layer (Bronze, Silver, and Gold), checking transformations and data integrity at every step.
- **Example Test**: `test_validate_bronze_data_format` validates the format of data in the Bronze layer.

---

## 2. Pipeline Failure Monitoring

To monitor for task failures within the Airflow DAG, configure retries and set up error-handling mechanisms as follows:

### A. Retry Configuration
- **Settings**: Use the `retries` and `retry_delay` parameters in the `default_args` of the Airflow DAG to specify the number of retries and delay between retries.
- **Example**:
  ```python
  default_args = {
      'owner': 'airflow',
      'email_on_failure': True,
      'retries': 3,
      'retry_delay': timedelta(minutes=5),
  }
  ```

### B. Task Failure Alerts
- **Implementation**: Configure Airflow to send an alert when a task fails. This can be achieved using the `email_on_failure` parameter in the `default_args`.
- **Custom Alerting**: For more flexibility, define an `on_failure_callback` function to send alerts to other channels (e.g., Slack or Microsoft Teams).

### C. Airflow Logs
- **Location**: Task logs are available in the Airflow UI and can be accessed by clicking on any task in the DAG's graph view.
- **Logs Directory**: Logs are stored in the `dags/logs` directory within the project structure.

---

## 3. Alerting System

The alerting system will notify the team when a critical issue is detected, such as a data quality problem or a pipeline failure.

### A. Email Notifications
- **Configuration**: Enable `email_on_failure` and `email_on_retry` in the DAG's `default_args` to send email notifications when a task fails or retries.
- **Example**:
  ```python
  default_args = {
      'owner': 'airflow',
      'email': ['team@example.com'],
      'email_on_failure': True,
      'email_on_retry': True,
      'retries': 3,
      'retry_delay': timedelta(minutes=5),
  }
  ```
- **Note**: Ensure that your Airflow SMTP configuration is set up correctly to send emails.

### B. Custom Notifications (Slack, Teams, etc.)
- **Implementation**: Use Airflow’s `SlackWebhookOperator` or custom `PythonOperator` to send notifications to Slack or Microsoft Teams.
- **Example**:
  ```python
  from airflow.operators.slack_webhook_operator import SlackWebhookOperator

  def send_slack_alert(context):
      slack_alert = SlackWebhookOperator(
          task_id='slack_alert',
          http_conn_id='slack_conn',
          message=":red_circle: Task Failed in DAG - {}".format(context['task_instance'].dag_id),
          username='airflow'
      )
      return slack_alert.execute(context=context)
  ```
- **Usage**: Add the `send_slack_alert` function as an `on_failure_callback` for tasks that require Slack alerts.

---

## Summary

By implementing data quality checks, configuring retries, and setting up notifications, this monitoring and alerting system ensures a robust and reliable data pipeline. The combination of email and custom notifications allows the team to stay informed about pipeline health and data quality.

For more details on setting up Airflow alerting, refer to the official [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/alerts.html).
