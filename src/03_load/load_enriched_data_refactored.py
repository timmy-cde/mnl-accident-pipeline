import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.cloud import storage
from dataclasses import dataclass

load_dotenv()

@dataclass
class LoadConfig:
    project_id: str
    dataset: str
    bucket_name: str
    clean_folder: str
    vehicles_folder: str
    bq_client: bigquery.Client
    gcs_client: storage.Client
    job_config_enriched: bigquery.LoadJobConfig
    job_config_vehicles: bigquery.LoadJobConfig

def init():
    """Initialize and return BigQuery/GCS configuration and clients.

    Reads environment variables for project, dataset, bucket, and cleaned data folder,
    then creates BigQuery and Cloud Storage clients with a load job configuration.

    Returns:
        LoadConfig: An instance of LoadConfig containing all initialized components
    """
    project_id = os.getenv("PROJECT_ID")
    dataset = os.getenv("DATASET")
    bucket_name = os.getenv('BUCKET_NAME')
    clean_folder = os.getenv('CLEANED_FOLDER_NAME')
    vehicles_folder = os.getenv('VEHICLES_FOLDER_NAME')

    bq_client = bigquery.Client()
    gcs_client = storage.Client()

    hive_partitioning_options_enriched = bigquery.HivePartitioningOptions()
    hive_partitioning_options_enriched.mode="AUTO"
    hive_partitioning_options_enriched.source_uri_prefix=f"gs://{bucket_name}/{clean_folder}/"

    job_config_enriched = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition="WRITE_APPEND",
            hive_partitioning=hive_partitioning_options_enriched
        )

    job_config_vehicles = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition="WRITE_APPEND",
        )

    return LoadConfig(
        project_id=project_id,
        dataset=dataset,
        bucket_name=bucket_name,
        clean_folder=clean_folder,
        vehicles_folder=vehicles_folder,
        bq_client=bq_client,
        gcs_client=gcs_client,
        job_config_enriched=job_config_enriched,
        job_config_vehicles=job_config_vehicles
    )

def get_current_enriched_path(bucket_name, cleaned_folder, vehicles_folder):
    """
    Get the GCS path for yesterday's enriched data
    
    Args:
        bucket_name: Name of the GCS bucket
        cleaned_folder: GCS folder path for cleaned data
        vehicles_folder: GCS folder path for vehicles data
        
    Returns:
        A dictionary of GCS file path for yesterday's data
    """
    PHT = ZoneInfo("Asia/Manila")
    now = datetime.now(PHT)
    yesterday = now - timedelta(days=1)

    year = yesterday.strftime("%Y")
    month = yesterday.strftime("%m")
    day = yesterday.strftime("%d")

    filepath_enriched = f"gs://{bucket_name}/{cleaned_folder}/date={year}-{month}-{day}/*.parquet"
    filepath_vehicles = f"gs://{bucket_name}/{vehicles_folder}/date={year}-{month}-{day}/*.parquet"

    return {
        "enriched": filepath_enriched,
        "vehicles": filepath_vehicles
    }


def submit_load_job(uris, project_id, staging_table_name, dataset, bq_client, job_config):
    """Submit a BigQuery load job for one or more URIs.

    Args:
        uris: Single URI string or list of URI strings to load.
        project_id: GCP project identifier.
        staging_table_name: Name of the staging table.
        dataset: BigQuery dataset name.
        bq_client: A google.cloud.bigquery.Client instance.
        job_config: BigQuery LoadJobConfig for the load operation.

    Returns:
        LoadJob: The submitted load job object.
    """
    # staging_table_id = f"{project_id}.{dataset}.staging_events"
    staging_table_id = f"{project_id}.{dataset}.{staging_table_name}"

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
    load_config = init()

    uris = get_current_enriched_path(load_config.bucket_name, load_config.clean_folder, load_config.vehicles_folder)

    load_job = submit_load_job(uris["enriched"], load_config.project_id, "staging_events", load_config.dataset, load_config.bq_client, load_config.job_config_enriched)
    load_job.result()

    print(f"Daily load completed: {load_job.output_rows} rows loaded for {uris['enriched']}.")

    load_job = submit_load_job(uris["vehicles"], load_config.project_id, "staging_vehicles", load_config.dataset, load_config.bq_client, load_config.job_config_vehicles)
    load_job.result()

    print(f"Daily load completed: {load_job.output_rows} rows loaded for {uris['vehicles']}.")


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
    load_config = init()

    # Build all URIs
    enriched_uris = []
    vehicle_uris = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date_dt:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        enriched_uri = f"gs://{load_config.bucket_name}/{load_config.clean_folder}/date={year}-{month}-{day}/*.parquet"
        enriched_uris.append(enriched_uri)

        vehicle_uri = f"gs://{load_config.bucket_name}/{load_config.vehicles_folder}/date={year}-{month}-{day}/*.parquet"
        vehicle_uris.append(vehicle_uri)

        current_date += timedelta(days=1)

    submit_batch_load_job(enriched_uris, batch_size, load_config, "staging_events", start_date, end_date)
    submit_batch_load_job(vehicle_uris, batch_size, load_config, "staging_vehicles", start_date, end_date)


def submit_batch_load_job(uris, batch_size, load_config, staging_table_name, start_date, end_date):
    """Submit a BigQuery load job for one or more URIs in batch mode.

    Args:
        uris: Single URI string or list of URI strings to load.
        batch_size: Number of URIs to include in each batch.
        load_config: An instance of LoadConfig containing project, dataset, clients, and job configurations.
        staging_table_name: Name of the staging table.
        start_date: Start date for the data range.
        end_date: End date for the data range.
    
    """
    # Submit load jobs in batches and collect them for parallel execution
    load_jobs = []
    
    for i in range(0, len(uris), batch_size):
        batch_uris = uris[i:i + batch_size]
        if staging_table_name == "staging_events":
            load_job = submit_load_job(batch_uris, load_config.project_id, staging_table_name, load_config.dataset, load_config.bq_client, load_config.job_config_enriched)

        if staging_table_name == "staging_vehicles":
            load_job = submit_load_job(batch_uris, load_config.project_id, staging_table_name, load_config.dataset, load_config.bq_client, load_config.job_config_vehicles)

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
    staging_table_id = f"{load_config.project_id}.{load_config.dataset}.{staging_table_name}"
    staging_table = load_config.bq_client.get_table(staging_table_id)

    print(f"\nHistorical load completed for {len(uris)} days")
    print(f"Total rows in staging table: {staging_table.num_rows}")
    print(f"Date range: {start_date} to {end_date}")



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