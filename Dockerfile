# Start from Airflow 3.x official image
# FROM apache/airflow:3.1.0-python3.11
FROM apache/airflow:3.2.0b2-python3.12

# Switch to root to install OS dependencies
USER root

# Install Firefox and Geckodriver for Selenium
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl firefox-esr \
        libdbus-glib-1-2 \
        libasound2 \
        libx11-xcb1 \
        libxt6 \
        libxrender1 \
        libxrandr2 \
        libgbm1 \
        libnss3 \
        libxss1 \
    && rm -fr /var/lib/apt/lists/* \
    && curl -L https://github.com/mozilla/geckodriver/releases/download/v0.36.0/geckodriver-v0.36.0-linux64.tar.gz | tar xz -C /usr/local/bin \
    && apt-get purge -y ca-certificates curl

# Set working directory for your ETL scripts
WORKDIR /app

# Copy ETL scripts and DAGs
COPY config/   /app/config/
COPY dags/     /opt/airflow/dags/
COPY sql/      /app/sql/
COPY src/      /app/src/

# Ensure airflow home is set
ENV AIRFLOW_HOME=/opt/airflow

# Switch back to airflow user
USER airflow

# Copy Python requirements and install
COPY requirements.prod.txt .
RUN pip install --no-cache-dir -r requirements.prod.txt

# Switch to root to install OS dependencies
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

RUN JAVA_PATH="$(dirname "$(dirname "$(readlink -f "$(which java)")")")" \
    && ln -s "$JAVA_PATH" /opt/java

ENV JAVA_HOME=/opt/java
ENV PATH=$JAVA_HOME/bin:$PATH

# Set PySpark Python environment
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Switch back to airflow user
USER airflow

# Default entrypoint: start airflow webserver and scheduler together
# ENTRYPOINT ["bash", "-c", "\
#     airflow db migrate && \
#     airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com --if-not-exists && \
#     airflow scheduler & \
#     airflow api-server"]
# ENTRYPOINT ["bash", "-c", "\
#     airflow db migrate && \
#     airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com && \
#     airflow scheduler"]

# Optional Cloud Run Adaptation
# Cloud Run works best for ephemeral execution.
# You can adapt this container to trigger a DAG on container start, run the ETL, and stop.
# Replace Airflow webserver startup with `airflow dags trigger <dag_id>` in `ENTRYPOINT`.