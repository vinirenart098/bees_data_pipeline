# Use the official Apache Airflow image as the base
FROM apache/airflow:2.5.0

# Switch to root user to install system dependencies
USER root

# Update packages and install Java (optional, only if needed for JDBC connections)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Java (if JDBC connectors are needed)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch to airflow user to install Python dependencies
USER airflow

# Install only the necessary Python dependencies for your pipeline
RUN pip install --no-cache-dir \
    requests \
    pandas \
    pyarrow \
    SQLAlchemy \
    psycopg2-binary \
    pymysql
