import os
from dotenv import load_dotenv
from google.cloud import bigquery
from litellm import query

load_dotenv()

def upload_to_bq(uri, project_id, dataset, job_config):
    client = bigquery.Client()

    staging_table_id = f"{project_id}.{dataset}.locations_staging"
    final_table_id = f"{project_id}.{dataset}.locations"

    load_job = client.load_table_from_uri(
        uri, staging_table_id, job_config=job_config
    )
    
    # Waits for the job to complete.
    load_job.result()

    # Get the staging table to check the number of rows loaded
    staging_table = client.get_table(staging_table_id)
    print("Loaded {} rows.".format(staging_table.num_rows))

    # Call the stored procedure to upsert data from staging to final table
    query = f"CALL `{project_id}.{dataset}.upsert_locations`()"

    # Execute the query
    client.query(query).result() 
    
    # Get the final table to check the number of rows
    final_table = client.get_table(final_table_id)
    print("Loaded {} rows.".format(final_table.num_rows))


def main():
    project_id = os.getenv("PROJECT_ID")
    dataset = os.getenv("DATASET")
    
    bucket_name = os.getenv("BUCKET_NAME")
    bucket_folder_name = os.environ.get("RAW_FOLDER_NAME")

    uri = f"gs://{bucket_name}/{bucket_folder_name}/locations/initial_locations.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    upload_to_bq(uri, project_id, dataset, job_config)

if __name__ == "__main__":
    main()