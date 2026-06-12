# mnl-accident-pipeline

Cloud schedule the compute at 8:30 AM daily, then trigger the Airflow

Initial Setup in Airflow

- apply Terraform infra (using Bash Operator)
- load stored procedures to bigquery (using Docker Operator)
- load initial locations to bigquery (using Docker Operator)
- ETL kaggle data (using Docker Operator)
- Seed operations for dim_directions

Daily Setup in Airflow

- ETL daily data (using Docker Operator)

To do:

- Create stored procedures for moving the enriched data to dim and fact tables
- Create separate docker images for:
  - Airflow
  - Initial setups for locations load and kaggle etl
  - transform stage
  - load stage
- Create starting script when the compute loads
  - programs that must be installed:
    - docker
    - terraform
