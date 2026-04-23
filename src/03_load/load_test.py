import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.cloud import storage

load_dotenv()

def init():
    """Initialize and return BigQuery/GCS configuration and clients.

    Reads environment variables for project, dataset, bucket, and cleaned data folder,
    then creates BigQuery and Cloud Storage clients with a load job configuration.

    Returns:
        tuple: project_id, dataset, bucket_name, clean_folder, bq_client, gcs_client, job_config
    """
    project_id = os.getenv("PROJECT_ID")
    dataset = os.getenv("DATASET")
    bucket_name = os.getenv('BUCKET_NAME')
    clean_folder = 'cleaned'

    bq_client = bigquery.Client()
    gcs_client = storage.Client()

    hive_partitioning_options = bigquery.HivePartitioningOptions()
    hive_partitioning_options.mode="AUTO"
    hive_partitioning_options.source_uri_prefix=f"gs://{bucket_name}/{clean_folder}/"


    job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition="WRITE_APPEND",
            hive_partitioning=hive_partitioning_options
        )

    return project_id, dataset, bucket_name, clean_folder, bq_client, gcs_client, job_config


def get_current_enriched_path(bucket_name, cleaned_folder):
    """
    Get the GCS path for yesterday's enriched data
    
    Args:
        bucket_name: Name of the GCS bucket
        cleaned_folder: GCS folder path for cleaned data
        
    Returns:
        GCS file path for yesterday's data
    """
    PHT = ZoneInfo("Asia/Manila")
    now = datetime.now(PHT)
    yesterday = now - timedelta(days=1)

    year = yesterday.strftime("%Y")
    month = yesterday.strftime("%m")
    day = yesterday.strftime("%d")

    filepath = f"gs://{bucket_name}/{cleaned_folder}/date={year}-{month}-{day}/*.parquet"
    return filepath


def gcs_path_exists(gcs_client, uri):
    """Check whether any Parquet files exist at the given GCS URI.

    Args:
        gcs_client: A google.cloud.storage.Client instance.
        uri: A GCS URI with optional wildcard pattern.

    Returns:
        bool: True if at least one .parquet blob exists under the URI prefix, else False.
    """
    # Remove "gs://"
    path = uri.replace("gs://", "")
    bucket_name, blob_path = path.split("/", 1)

    # Extract prefix before wildcard
    prefix = blob_path.split("*")[0]

    bucket = gcs_client.bucket(bucket_name)
    blobs = gcs_client.list_blobs(bucket, prefix=prefix)

    # Check if any blob matches full pattern
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            return True

    return False


def upload_to_bq(uri, project_id, dataset, bq_client, gcs_client, job_config):
    """Load Parquet files from GCS into the BigQuery staging table.

    Args:
        uri: The GCS URI pointing to Parquet files.
        project_id: GCP project identifier.
        dataset: BigQuery dataset name.
        bq_client: A google.cloud.bigquery.Client instance.
        gcs_client: A google.cloud.storage.Client instance.
        job_config: BigQuery LoadJobConfig for the load operation.
    """

    if not gcs_path_exists(gcs_client, uri):
        print(f"No files found at {uri}. Skipping load.")
        return

    staging_table_id = f"{project_id}.{dataset}.test_staging_enriched"

    load_job = bq_client.load_table_from_uri(
        uri, staging_table_id, job_config=job_config
    )
    
    # Waits for the job to complete.
    load_job.result()

    # Get the staging table to check the number of rows loaded
    staging_table = bq_client.get_table(staging_table_id)
    print(f"{uri}: loaded {staging_table.num_rows} rows in {staging_table_id}.")

    # TODO
    # Run stored procedures to update the dim and fact tables


def load_daily_data():
    """Load yesterday's enriched data from GCS into BigQuery.

    This function initializes the environment and clients, computes the
    GCS path for yesterday's data, and invokes the load process.
    """
    project_id, dataset, bucket_name, clean_folder, bq_client, gcs_client, job_config = init()

    enriched_uri = get_current_enriched_path(bucket_name, clean_folder)
    
    upload_to_bq(enriched_uri, project_id, dataset, bq_client, gcs_client, job_config)

    print(f"Daily load completed for {enriched_uri}.")


def load_historical_data(start_date, end_date):
    """Load a range of historical enriched data from GCS into BigQuery.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
    """
    project_id, dataset, bucket_name, clean_folder, bq_client, gcs_client, job_config = init()

    enriched_uris = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date_dt:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        enriched_uri = f"gs://{bucket_name}/{clean_folder}/Date={year}-{month}-{day}/*.parquet"
        enriched_uris.append(enriched_uri)

        current_date += timedelta(days=1)

    for uri in enriched_uris:
        upload_to_bq(uri, project_id, dataset, bq_client, gcs_client, job_config)

    print(f"Historical load completed for {len(enriched_uris)} files from {start_date} to {end_date}.")


def main():
    """
    Main entry point. Runs in daily mode by default, or batch mode if
    START_DATE and END_DATE environment variables are set.
    """
    start_date = "2026-03-15"
    end_date = "2026-03-29"

    if start_date and end_date:
        load_historical_data(start_date, end_date)
    else:
        load_daily_data()


if __name__ == "__main__":
    main()