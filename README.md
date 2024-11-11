
# 🚀 Project - Data Pipeline with Airflow

## 📄 Overview
This project sets up a data pipeline using Apache Airflow and Docker. The goal is to orchestrate the data flow through various stages, including extraction, transformation, and loading. We use a Medallion architecture to organize data into layers (Bronze, Silver, Gold) for better management and transformation.

## 📂 Directory Structure
The following is the structure of the main project directory:

```
<project-root>
├── config/                   # Configuration files for the project
│   └── Dockerfile            # Docker configuration file
├── dags/                     # Directory containing Airflow DAGs
│   ├── data/                 # Directory for storing data files (raw, transformed)
│   ├── logs/                 # Directory for Airflow logs
│   └── brewery_dag.py        # Main DAG script
├── notebooks/                # Jupyter notebooks for data analysis and quality checks
│   └── visualizer.ipynb      # Notebook for visualization and data inspection
├── plugins/                  # Custom plugins for Airflow
├── scripts/                  # Scripts for testing and supporting pipeline tasks
├── .gitignore                # Git ignore file
├── readme-notifications and monitoring.txt  # Documentation for notifications and monitoring
└── requirements.txt          # Python dependencies for Airflow and project
```

## 📑 Table of Contents
1. [Required Applications and Libraries](#required-applications-and-libraries)
2. [Installation and Configuration Guide](#installation-and-configuration-guide)
3. [Starting Docker and Useful Commands](#starting-docker-and-useful-commands)
4. [Running Airflow and Triggering the DAG](#running-airflow-and-triggering-the-dag)
5. [Running Unit Tests and Checking Notebooks](#running-unit-tests-and-checking-notebooks)
6. [Shutting Down the Program](#shutting-down-the-program)
7. [Troubleshooting](#troubleshooting)
8. [Conclusion](#conclusion)

---

## 🛠 Required Applications and Libraries

To run this project, make sure to have the following applications installed:
- **[Docker](https://www.docker.com/)**: To create and manage containers.
- **[Docker Compose](https://docs.docker.com/compose/)**: To orchestrate multiple Docker containers.
- **[Python 3.x](https://www.python.org/downloads/)**: To run scripts and unit tests.
- **[Apache Airflow](https://airflow.apache.org/)**: Installed in the Docker container, configured to run the pipeline.
- **Python Libraries**: Listed in the `requirements.txt` file and will be installed automatically in the Airflow container.

---

## 📋 Installation and Configuration Guide

1. **Clone the Repository**:
   Make sure to run the following command in a terminal with **administrator privileges**:
   ```bash
   git clone https://github.com/vinirenart098/bees_data_pipeline
   cd bees_data_pipeline\root\config
   ```
   
2. **Initial Configuration**:
   Check the `.env` file to adjust necessary environment variables, such as credentials and database settings, if applicable.

---

## 🚀 Starting Docker and Useful Commands

To start the Docker containers and prepare the environment:

1. **Build and Start Containers**:
   Run these commands in a terminal with **administrator privileges**:
   ```bash
   docker-compose build
   docker-compose up
   ```
2. **Useful Commands**:
   - To check container status (run with **administrator privileges**):
     ```bash
     docker ps
     ```
   - To stop containers:
     ```bash
     docker-compose down
     ```

---

## 🌐 Running Airflow and Triggering the DAG

### 1. Accessing the Airflow Interface
After starting the containers, you can access Apache Airflow via web at:
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Credentials**:
  - **Username**: airflow
  - **Password**: airflow

### 2. Triggering the DAG
1. Log in to the Airflow interface.
2. Go to the **DAGs** tab.
3. Locate and click on the `brewery_data_pipeline_medallion` DAG.
4. Click on **Trigger DAG** to start the pipeline.

### 3. Verifying DAG Execution
- Go to the task graph view in the Airflow interface.
- Click on each task to view logs and confirm successful execution.

---

## 🧪 Running Unit Tests and Checking Notebooks

### 1. Running Unit Tests
To ensure the pipeline is working correctly, run the unit tests with the command:

```bash
python root/scripts/test_pipeline.py
```

This will validate the functions for data extraction, transformation, and loading.

### 2. Checking Notebooks
There are Jupyter Notebooks for data quality analysis and verification. You can access them in the following directory:

```bash
root/notebooks/
```

---

## 🔄 Shutting Down the Program

1. **Stopping Docker**:
   To stop all containers, run:

   ```bash
   docker-compose down
   ```

2. **Cleaning Up Unused Containers and Images**:
   Optionally, you can clean up old containers and images with:

   ```bash
   docker system prune -a
   ```

---

## 🛠️ Troubleshooting

### Issue 1: Docker Containers Not Starting
**Solution**: Ensure Docker is installed and running on your system. Restart the Docker service if necessary.

### Issue 2: Airflow Web UI Not Loading
**Solution**: Check if all services in Docker are running correctly. You can restart the services with:

```bash
docker-compose down
docker-compose up
```

### Issue 3: Pipeline Fails During Execution
**Solution**: Review the logs in the Airflow UI. If the error is related to missing data or API failures, verify your API configurations and the data availability.

### Issue 4: Permission Issues on Data Files
**Solution**: Ensure that your Docker containers have proper permissions to access and write to the `/opt/airflow/dags/data/` directory.

---

## ✅ Conclusion
This README provides all necessary instructions to set up, run, and monitor the data pipeline with Airflow. Follow the steps above to ensure the project runs correctly and refer to the Troubleshooting section to resolve any potential issues.

For further information or questions, please contact the project maintainer.
