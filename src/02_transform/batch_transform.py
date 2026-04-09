import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from google.cloud import storage

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

from utils.PostParser import post_parser
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

# -------------------------------------------------------------------------------------------
def generate_filenames(scrape_folder, start_date, end_date):
    """
    Generates list of GCS filenames between start_date and end_date (inclusive)
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

# -------------------------------------------------------------------------------------------
def main(start_date, end_date):

    spark = SparkSession.builder \
            .master("local[*]") \
            .appName('Transform Stage (Batch)') \
            .config("spark.jars", f"{gcs_connector_path},{bigquery_connector_path}") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .getOrCreate() 

    project_id = os.getenv("PROJECT_ID")
    dataset = os.getenv("DATASET")
    locations_table_id = f"{project_id}:{dataset}.locations"
    staging_locations_table_id = f"{project_id}:{dataset}.locations_staging"
    bucket_name = os.getenv('BUCKET_NAME')
    raw_folder = os.getenv('RAW_FOLDER_NAME')
    clean_folder = os.getenv('CLEANED_FOLDER_NAME')

    scrape_folder = f"{raw_folder}/scrape"

    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    # Load locations once (reuse for all files)
    df_locations = load_locations_df(spark, locations_table_id)

    filenames = generate_filenames(scrape_folder, start_date, end_date)

    for raw_filename in filenames:
        print(f"Processing: {raw_filename}")

        blob = storage.Blob(bucket=bucket, name=raw_filename)

        if not blob.exists():
            print(f"Skipping (not found): {raw_filename}")
            continue

        df_raw = gcs_file_read(spark, bucket_name, raw_filename, RawSchema)

        # Step 1: parse
        df_partial_parsed = partial_parse_raw_data(df_raw)

        # Step 2: enrich from BQ
        df_full_parsed = get_locations_from_bq(df_locations, df_partial_parsed)

        # Step 3: missing locations
        missing_locations = get_missing_locations(df_full_parsed)

        if len(missing_locations) != 0:
            resolved_locations_df = get_batch_geocode(spark, missing_locations)

            update_locations_bq(resolved_locations_df, staging_locations_table_id, project_id, dataset)

            # reload updated locations
            df_locations = load_locations_df(spark, locations_table_id)

            df_full_parsed = df_full_parsed.drop("city", "latitude", "longitude", "high_accuracy")
            df_full_parsed = get_locations_from_bq(df_locations, df_full_parsed)

        df_final = df_full_parsed.select(
            'date', 'time', 'city', 'location',
            'latitude', 'longitude', 'high_accuracy',
            'direction', 'type', 'lanes_blocked',
            'involved', 'tweet', 'source'
        )

        # Step 4: write per file
        gcs_upload_parquet(bucket_name, clean_folder, df_final)

    print("Batch processing complete.")


if __name__ == "__main__":
    START_DATE = os.getenv("START_DATE")
    END_DATE = os.getenv("END_DATE")
    main(START_DATE, END_DATE)