# Start from Airflow 3.x official image
# FROM apache/airflow:3.1.0-python3.11
FROM apache/airflow:3.2.0b2-python3.12

# Switch to root to install OS dependencies
USER root

# Install Java 11 (Temurin) and other OS packages
RUN apt-get update && apt-get install -y \
    wget gnupg curl unzip git && \
    mkdir -p /etc/apt/keyrings && \
    wget -O- https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor > /etc/apt/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && apt-get install -y temurin-11-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set PySpark Python environment
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Set working directory for your ETL scripts
WORKDIR /app

# Copy ETL scripts and DAGs
COPY src/      /app/src/
COPY config/   /app/config/
COPY dags/     /opt/airflow/dags/

# Ensure airflow home is set
ENV AIRFLOW_HOME=/opt/airflow

# Switch back to airflow user
USER airflow

# Copy Python requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Default entrypoint: start airflow webserver and scheduler together
# ENTRYPOINT ["bash", "-c", "\
#     airflow db migrate && \
#     airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com --if-not-exists && \
#     airflow scheduler & \
#     airflow api-server"]
ENTRYPOINT ["bash", "-c", "\
    airflow db migrate && \
    airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com && \
    airflow scheduler"]

# Optional Cloud Run Adaptation
# Cloud Run works best for ephemeral execution.
# You can adapt this container to trigger a DAG on container start, run the ETL, and stop.
# Replace Airflow webserver startup with `airflow dags trigger <dag_id>` in `ENTRYPOINT`.