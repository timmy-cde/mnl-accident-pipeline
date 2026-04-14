# mnl-accident-pipeline

Cloud schedule the compute at 8:30 AM daily, then trigger the Airflow

Initial Setup in Airflow

- apply Terraform infra
- load stored procedures to bigquery
- load initial locations to bigquery
- ETL kaggle data

Daily Setup in Airflow

- ETL daily data
