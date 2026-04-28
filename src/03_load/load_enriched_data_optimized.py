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
    clean_folder = os.getenv('CLEANED_FOLDER_NAME')

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


def submit_load_job(uris, project_id, dataset, bq_client, job_config):
    """Submit a BigQuery load job for one or more URIs.

    Args:
        uris: Single URI string or list of URI strings to load.
        project_id: GCP project identifier.
        dataset: BigQuery dataset name.
        bq_client: A google.cloud.bigquery.Client instance.
        job_config: BigQuery LoadJobConfig for the load operation.

    Returns:
        LoadJob: The submitted load job object.
    """
    staging_table_id = f"{project_id}.{dataset}.staging_enriched"

    # Convert single URI to list for uniform handling
    if isinstance(uris, str):
        uris = [uris]

    load_job = bq_client.load_table_from_uri(
        uris, staging_table_id, job_config=job_config
    )
    
    return load_job


def load_daily_data():
    """Load yesterday's enriched data from GCS into BigQuery.

    This function initializes the environment and clients, computes the
    GCS path for yesterday's data, and invokes the load process.
    """
    project_id, dataset, bucket_name, clean_folder, bq_client, gcs_client, job_config = init()

    enriched_uri = get_current_enriched_path(bucket_name, clean_folder)
    
    load_job = submit_load_job(enriched_uri, project_id, dataset, bq_client, job_config)
    load_job.result()

    print(f"Daily load completed: {load_job.output_rows} rows loaded for {enriched_uri}.")


def load_historical_data(start_date, end_date, batch_size=14):
    """Load a range of historical enriched data from GCS into BigQuery in parallel.

    Submits multiple load jobs concurrently to maximize throughput.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        batch_size: Number of days to batch into a single load job (default 14).
                   Larger batches = fewer jobs but each takes longer.
                   Smaller batches = more parallelism but more overhead.
    """
    project_id, dataset, bucket_name, clean_folder, bq_client, gcs_client, job_config = init()

    # Build all URIs
    enriched_uris = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date_dt:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        enriched_uri = f"gs://{bucket_name}/{clean_folder}/date={year}-{month}-{day}/*.parquet"
        enriched_uris.append(enriched_uri)

        current_date += timedelta(days=1)

    # Submit load jobs in batches and collect them for parallel execution
    load_jobs = []
    
    for i in range(0, len(enriched_uris), batch_size):
        batch_uris = enriched_uris[i:i + batch_size]
        load_job = submit_load_job(batch_uris, project_id, dataset, bq_client, job_config)
        load_jobs.append((load_job, batch_uris))
        print(f"Submitted job {len(load_jobs)} with {len(batch_uris)} days of data")

    # Wait for all jobs to complete and collect results
    print(f"\nWaiting for {len(load_jobs)} parallel load jobs to complete...")
    total_rows = 0
    for idx, (load_job, uris) in enumerate(load_jobs, 1):
        load_job.result()  # This blocks until the job completes
        total_rows += load_job.output_rows
        print(f"Job {idx}/{len(load_jobs)} completed: {load_job.output_rows} rows loaded")

    # Get final table stats
    staging_table_id = f"{project_id}.{dataset}.staging_enriched"
    staging_table = bq_client.get_table(staging_table_id)
    
    print(f"\n✓ Historical load completed for {len(enriched_uris)} days")
    print(f"✓ Total rows in staging table: {staging_table.num_rows}")
    print(f"✓ Date range: {start_date} to {end_date}")


def main():
    """
    Main entry point. Runs in daily mode by default, or batch mode if
    START_DATE and END_DATE environment variables are set.
    """
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    if start_date and end_date:
        load_historical_data(start_date, end_date)
    else:
        load_daily_data()


if __name__ == "__main__":
    main()