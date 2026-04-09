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

# -------------------------------------------------------------------------------------------
def get_current_raw_filename(scrape_folder):
    PHT = ZoneInfo("Asia/Manila")
    now = datetime.now(PHT)
    yesterday = now - timedelta(days=1)

    year = yesterday.strftime("%Y")
    month = yesterday.strftime("%m")
    day = yesterday.strftime("%d")

    filename = f"{scrape_folder}/{year}/{month}/scrape_data_{year}{month}{day}.csv"
    return filename

# -------------------------------------------------------------------------------------------
def main():
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName('Transform Stage (Daily)') \
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
    
    df_locations = load_locations_df(spark, locations_table_id)
    raw_filename = get_current_raw_filename(scrape_folder)
    is_file_exists = storage.Blob(bucket=bucket, name=raw_filename).exists()

    if not is_file_exists:
        return f"{raw_filename} does not exists"
    
    df_raw = gcs_file_read(spark, bucket_name, raw_filename, RawSchema)
    
    # Get transformations that do not connect to APIs
    df_partial_parsed = partial_parse_raw_data(df_raw)

    # Add the location details (City, Longitude, Latitude, Accuracy) vis Locations db in bigquery
    df_full_parsed = get_locations_from_bq(df_locations, df_partial_parsed)

    # get missing locations
    missing_locations = get_missing_locations(df_full_parsed)

    if len(missing_locations) != 0:
        resolved_locations_df = get_batch_geocode(spark, missing_locations)
        
        # update locations df in bq
        update_locations_bq(resolved_locations_df, staging_locations_table_id, project_id, dataset)
        
        # reload locations df in bq
        df_locations = load_locations_df(spark, locations_table_id)

        # re-join from df_full_parsed
        df_full_parsed = df_full_parsed.drop("city", "latitude", "longitude", "high_accuracy")
        df_full_parsed = get_locations_from_bq(df_locations, df_full_parsed)

    df_final = df_full_parsed.select(
        'date', 'time', 'city', 'location',
        'latitude', 'longitude', 'high_accuracy',
        'direction', 'type', 'lanes_blocked',
        'involved', 'tweet', 'source'
    )
    # upload to gcs as parquet
    gcs_upload_parquet(bucket_name, clean_folder, df_final)

    print(f"{datetime.now(ZoneInfo("Asia/Manila")).strftime("%Y-%m-%d")} daily processing complete.")

if __name__ == "__main__":
    main()