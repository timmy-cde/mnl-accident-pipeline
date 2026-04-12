import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from google.cloud import storage

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType

from utils.LocationFunctions import load_locations_df, get_locations_from_bq, get_missing_locations, get_batch_geocode, update_locations_bq
from utils.Common import gcs_file_read, gcs_upload_parquet, partial_parse_raw_data

gcs_connector_path = '../../config/gcs-connector-hadoop3-latest.jar'
bigquery_connector_path = '../../config/spark-bigquery-with-dependencies_2.12-0.35.0.jar'

load_dotenv()

RawSchema = StructType([
    StructField('content', StringType(), True),
    StructField('tweetlinkid', StringType(), True),
    StructField('created_at', DateType(), True),
])

def initialize(app_name):
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName(app_name) \
            .config("spark.jars", f"{gcs_connector_path},{bigquery_connector_path}") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .getOrCreate()
    
    gcs_client = storage.Client()

    project_id = os.getenv("PROJECT_ID")
    dataset = os.getenv("DATASET")
    locations_table_id = f"{project_id}:{dataset}.locations"
    staging_locations_table_id = f"{project_id}:{dataset}.locations_staging"
    bucket_name = os.getenv('BUCKET_NAME')
    raw_folder = os.getenv('RAW_FOLDER_NAME')
    clean_folder = os.getenv('CLEANED_FOLDER_NAME')
    scrape_folder = f"{raw_folder}/scrape"

    df_locations = load_locations_df(spark, locations_table_id)

    return spark, gcs_client, project_id, dataset, locations_table_id, staging_locations_table_id, bucket_name, raw_folder, clean_folder, scrape_folder, df_locations

    
def generate_filenames(scrape_folder, start_date, end_date):
    """
    Generates list of GCS filenames between start_date and end_date (inclusive)
    
    Args:
        scrape_folder: GCS folder path for scrape data
        start_date: Start date string in "YYYY-MM-DD" format
        end_date: End date string in "YYYY-MM-DD" format
        
    Returns:
        List of GCS file paths
    """
    PHT = ZoneInfo("Asia/Manila")

    start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=PHT)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=PHT)

    current = start
    filenames = []

    while current <= end:
        year = current.strftime("%Y")
        month = current.strftime("%m")
        day = current.strftime("%d")

        filename = f"{scrape_folder}/{year}/{month}/scrape_data_{year}{month}{day}.csv"
        filenames.append(filename)

        current += timedelta(days=1)

    return filenames


def get_current_raw_filename(scrape_folder):
    """
    Get the GCS filename for yesterday's scrape data
    
    Args:
        scrape_folder: GCS folder path for scrape data
        
    Returns:
        GCS file path for yesterday's data
    """
    PHT = ZoneInfo("Asia/Manila")
    now = datetime.now(PHT)
    yesterday = now - timedelta(days=1)

    year = yesterday.strftime("%Y")
    month = yesterday.strftime("%m")
    day = yesterday.strftime("%d")

    filename = f"{scrape_folder}/{year}/{month}/scrape_data_{year}{month}{day}.csv"
    return filename


def process_single_file(spark, gcs_client, bucket_name, raw_filename, df_locations, locations_table_id, staging_locations_table_id, project_id, dataset):
    """
    Process a single raw data file through the transformation pipeline
    
    Args:
        spark: SparkSession
        bucket_name: GCS bucket name
        raw_filename: GCS path to raw file
        df_locations: Locations DataFrame
        locations_table_id: BigQuery locations table ID
        staging_locations_table_id: BigQuery staging locations table ID
        project_id: GCP project ID
        dataset: BigQuery dataset name
        
    Returns:
        Processed DataFrame or None if file doesn't exist
    """
    # gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)
    
    blob = storage.Blob(bucket=bucket, name=raw_filename)
    if not blob.exists():
        print(f"Skipping (not found): {raw_filename}")
        return None

    print(f"Processing: {raw_filename}")
    df_raw = gcs_file_read(spark, bucket_name, raw_filename, RawSchema)

    # Step 1: parse
    df_partial_parsed = partial_parse_raw_data(df_raw)

    # Step 2: enrich from BQ
    df_full_parsed = get_locations_from_bq(df_locations, df_partial_parsed)

    # Step 3: handle missing locations
    missing_locations = get_missing_locations(df_full_parsed)

    if len(missing_locations) != 0:
        resolved_locations_df = get_batch_geocode(spark, missing_locations)

        update_locations_bq(resolved_locations_df, staging_locations_table_id, project_id, dataset)

        # reload updated locations
        df_locations = load_locations_df(spark, locations_table_id)

        df_full_parsed = df_full_parsed.drop("city", "latitude", "longitude", "high_accuracy")
        df_full_parsed = get_locations_from_bq(df_locations, df_full_parsed)

    df_final = df_full_parsed.select(
        'date', 'year', 'month', 'day', 'week', 'weekday', 
        'time', 'hour', 'city', 'location',
        'latitude', 'longitude', 'high_accuracy',
        'direction', 'type', 'lanes_blocked',
        'involved', 'tweet', 'source'
    )

    return df_final


def run_daily_transform():
    """Run daily transformation for yesterday's data"""
    spark, gcs_client, project_id, dataset, locations_table_id, staging_locations_table_id, bucket_name, raw_folder, clean_folder, scrape_folder, df_locations = initialize('Transform Stage (Daily)')

    raw_filename = get_current_raw_filename(scrape_folder)
    
    df_final = process_single_file(
        spark, gcs_client, bucket_name, raw_filename, df_locations,
        locations_table_id, staging_locations_table_id, project_id, dataset
    )
    
    if df_final is None:
        print(f"{raw_filename} does not exist")
        return

    # upload to gcs as parquet
    gcs_upload_parquet(bucket_name, clean_folder, df_final)

    print(f"{datetime.now(ZoneInfo('Asia/Manila')).strftime('%Y-%m-%d')} daily processing complete.")


def run_batch_transform(start_date, end_date):
    """Run batch transformation for a date range"""
    spark, gcs_client, project_id, dataset, locations_table_id, staging_locations_table_id, bucket_name, raw_folder, clean_folder, scrape_folder, df_locations = initialize('Transform Stage (Batch)')

    filenames = generate_filenames(scrape_folder, start_date, end_date)

    for raw_filename in filenames:
        df_final = process_single_file(
            spark, gcs_client, bucket_name, raw_filename, df_locations,
            locations_table_id, staging_locations_table_id, project_id, dataset
        )
        
        if df_final is not None:
            # upload to gcs as parquet
            gcs_upload_parquet(bucket_name, clean_folder, df_final)

    print("Batch processing complete.")


def main():
    """
    Main entry point. Runs in daily mode by default, or batch mode if
    START_DATE and END_DATE environment variables are set.
    """
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    if start_date and end_date:
        run_batch_transform(start_date, end_date)
    else:
        run_daily_transform()


if __name__ == "__main__":
    main()