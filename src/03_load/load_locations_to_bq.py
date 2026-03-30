import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

def upload_to_bq(uri, table_id, job_config):
    """
    uri: gcs file location
    table_id: project_id.dataset_name.table_name
    job_config: bigquery.LoadJobConfig options
    """

    client = bigquery.Client()

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    
    # Waits for the job to complete.
    load_job.result()

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def main():
    project_id = os.getenv("PROJECT_ID")
    dataset = os.getenv("DATASET")
    table_id = f"{project_id}.{dataset}.locations"

    bucket_name = os.getenv("BUCKET_NAME")
    bucket_folder_name = os.environ.get("RAW_FOLDER_NAME")

    uri = f"gs://{bucket_name}/{bucket_folder_name}/locations/initial_locations.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    upload_to_bq(uri, table_id, job_config)

if __name__ == "__main__":
    main()