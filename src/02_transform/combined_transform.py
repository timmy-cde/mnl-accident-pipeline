import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dataclasses import dataclass

from google.cloud import storage

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StructType, StructField, IntegerType, StringType, DateType

from utils.LocationFunctions import load_locations_df, get_locations_from_bq, get_missing_locations, get_batch_geocode, update_locations_bq
from utils.Common import gcs_file_read, gcs_upload_parquet, partial_parse_raw_data, parse_involved_data
from utils.VehicleFunctions import load_vehicles_df, add_vehicle_suggestions

gcs_connector_path = '../../config/gcs-connector-hadoop3-latest.jar'
bigquery_connector_path = '../../config/spark-bigquery-with-dependencies_2.12-0.35.0.jar'

load_dotenv()

ScrapedRawSchema = StructType([
    StructField('content', StringType(), True),
    StructField('tweetlinkid', StringType(), True),
    StructField('created_at', DateType(), True),
])

KaggleRawSchema = StructType([
    StructField('Date', DateType(), True),
    StructField('Time', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('Latitude', DoubleType(), True),
    StructField('Longitude', DoubleType(), True),
    StructField('High_Accuracy', DoubleType(), True),
    StructField('Direction', StringType(), True),
    StructField('Type', StringType(), True),
    StructField('Lanes_Blocked', IntegerType(), True),
    StructField('Involved', StringType(), True),
    StructField('Tweet', StringType(), True),
    StructField('Source', StringType(), True),
])

@dataclass
class TransformContext:
    spark: any
    gcs_client: any
    project_id: str
    dataset: str
    locations_table_id: str
    vehicle_table_id: str
    staging_locations_table_id: str
    bucket_name: str
    raw_folder: str
    clean_folder: str
    vehicles_folder: str
    scrape_folder: str
    df_locations: any
    df_vehicles: any

def initialize(app_name, data_source='scraped'):
    """Initialize Spark, GCS client, and location references for the transform pipeline.

    Args:
        app_name: Name for the Spark application.
        data_source: Data source type ('scraped' or 'kaggle').

    Returns:
        TransformContext: spark, gcs_client, project_id, dataset, locations_table_id,
               staging_locations_table_id, bucket_name, raw_folder,
               clean_folder, scrape_folder, df_locations, df_vehicles
    """
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
    locations_table_id = f"{project_id}:{dataset}.dim_locations"
    vehicle_table_id = f"{project_id}:{dataset}.dim_vehicles"
    staging_locations_table_id = f"{project_id}:{dataset}.staging_locations"
    bucket_name = os.getenv('BUCKET_NAME')
    raw_folder = os.getenv('RAW_FOLDER_NAME')
    clean_folder = os.getenv('CLEANED_FOLDER_NAME')
    vehicles_folder = os.getenv('VEHICLES_FOLDER_NAME')
    scrape_folder = f"{raw_folder}/scrape"

    df_locations = load_locations_df(spark, locations_table_id)
    df_vehicles = load_vehicles_df(spark, vehicle_table_id)

    return TransformContext(
        spark=spark,
        gcs_client=gcs_client,
        project_id=project_id,
        dataset=dataset,
        locations_table_id=locations_table_id,
        vehicle_table_id=vehicle_table_id,
        staging_locations_table_id=staging_locations_table_id,
        bucket_name=bucket_name,
        raw_folder=raw_folder,
        clean_folder=clean_folder,
        vehicles_folder=vehicles_folder,
        scrape_folder=scrape_folder,
        df_locations=df_locations,
        df_vehicles=df_vehicles,
    )

    
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
        filenames.append(build_raw_filename(scrape_folder, current))
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

    return build_raw_filename(scrape_folder, yesterday)


def build_raw_filename(scrape_folder, dt):
    """Builds a raw scrape filename from a datetime-like object.

    Args:
        scrape_folder: base folder path
        dt: datetime or date object

    Returns:
        filename string
    """
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    return f"{scrape_folder}/{year}/{month}/scrape_data_{year}{month}{day}.csv"


def process_df(spark, df_raw, df_locations, df_vehicles, locations_table_id, staging_locations_table_id, project_id, dataset):
    """
    Process a DataFrame through the transformation pipeline
    
    Args:
        spark: SparkSession
        df_raw: Raw DataFrame
        df_locations: Locations DataFrame
        df_vehicles: Vehicles DataFrame
        locations_table_id: BigQuery locations table ID
        staging_locations_table_id: BigQuery staging locations table ID
        project_id: GCP project ID
        dataset: BigQuery dataset name
        
    Returns:
        Processed DataFrame
    """
    # parse
    df_partial_parsed = partial_parse_raw_data(df_raw)

    # handle missing timestamps
    null_timestamp_count = df_partial_parsed.filter(F.col("event_timestamp").isNull()).count()
    if null_timestamp_count > 0:
        window = Window.rowsBetween(Window.unboundedPreceding, 0)
        df_partial_parsed = df_partial_parsed.withColumn("time", F.last("time", ignorenulls=True).over(window))
        df_partial_parsed = df_partial_parsed.withColumn(
            "event_timestamp", 
            F.to_timestamp(F.concat_ws(' ', F.col("date"), F.col("time")), "yyyy-MM-dd HH:mm")
        )

    # enrich from BQ
    df_full_parsed = get_locations_from_bq(df_locations, df_partial_parsed)

    # handle missing locations
    missing_locations = get_missing_locations(df_full_parsed)

    if len(missing_locations) != 0:
        resolved_locations_df = get_batch_geocode(spark, missing_locations)

        update_locations_bq(resolved_locations_df, staging_locations_table_id, project_id, dataset)

        # reload updated locations
        df_locations = load_locations_df(spark, locations_table_id)

        df_full_parsed = df_full_parsed.drop("location_id", "city", "latitude", "longitude", "accuracy")
        df_full_parsed = get_locations_from_bq(df_locations, df_full_parsed)

    df_final = df_full_parsed.select(
        'event_id', 'date', 'time', 'event_timestamp', 'location_id',
        'city', 'location', 'latitude', 'longitude', 'accuracy',
        'direction', 'type', 'lanes_blocked',
        'involved', 'post', 'link'
    )

    df_involved = parse_involved_data(df_final)
    df_involved_enriched = add_vehicle_suggestions(df_involved, df_vehicles)

    return df_final, df_involved_enriched


def process_single_file(context, raw_filename):
    """
    Process a single raw data file through the transformation pipeline
    
    Args:
        context: TransformContext containing Spark, GCS client, and reference data
        raw_filename: GCS path to raw file
        
    Returns:
        Processed DataFrame or None if file doesn't exist
    """
    df_raw = load_raw_if_exists(context.spark, context.gcs_client, context.bucket_name, raw_filename, ScrapedRawSchema)
    if df_raw is None:
        return None

    df_final, df_involved = process_df(
        context.spark,
        df_raw,
        context.df_locations,
        context.df_vehicles,
        context.locations_table_id,
        context.staging_locations_table_id,
        context.project_id,
        context.dataset,
    )

    return df_final, df_involved


def process_kaggle_file(context, raw_filename):
    """
    Process the Kaggle raw data file through the transformation pipeline
    
    Args:
        context: TransformContext containing Spark, GCS client, and reference data
        raw_filename: GCS path to raw file
        
    Returns:
        Processed DataFrame or None if file doesn't exist
    """
    df_raw = load_raw_if_exists(context.spark, context.gcs_client, context.bucket_name, raw_filename, KaggleRawSchema)
    if df_raw is None:
        return None

    df_raw_specific = df_raw.select("Tweet", "Date", "Source")
    df_raw_renamed = df_raw_specific.withColumnsRenamed({"Tweet": "content", "Date": "created_at", "Source": "tweetlinkid"})

    df_final, df_involved = process_df(
        context.spark,
        df_raw_renamed,
        context.df_locations,
        context.df_vehicles,
        context.locations_table_id,
        context.staging_locations_table_id,
        context.project_id,
        context.dataset,
    )

    return df_final, df_involved


def load_raw_if_exists(spark, gcs_client, bucket_name, raw_filename, schema):
    """Helper to check for GCS blob existence and read it.

    Returns DataFrame if exists, otherwise None.
    """
    bucket = gcs_client.bucket(bucket_name)
    blob = storage.Blob(bucket=bucket, name=raw_filename)
    if not blob.exists():
        print(f"Skipping (not found): {raw_filename}")
        return None

    print(f"Processing: {raw_filename}")
    return gcs_file_read(spark, bucket_name, raw_filename, schema)


def upload_results(bucket_name, clean_folder, vehicles_folder, df_final, df_involved):
    """Upload final and involved dataframes to GCS as parquet, if present."""
    if df_final is not None:
        gcs_upload_parquet(bucket_name, clean_folder, df_final, is_vehicle_df=False)

    if df_involved is not None:
        gcs_upload_parquet(bucket_name, vehicles_folder, df_involved, is_vehicle_df=True)


def run_daily_transform():
    """Run daily transformation for yesterday's data.

    This function initializes Spark and GCS, locates yesterday's raw scrape file,
    transforms it, and uploads the cleaned parquet output to GCS.
    """
    ctx = initialize('Transform Stage (Daily)', 'scraped')

    raw_filename = get_current_raw_filename(ctx.scrape_folder)

    df_final, df_involved = process_single_file(ctx, raw_filename)
    
    if df_final is None:
        print(f"{raw_filename} does not exist")
        return
    

    # upload to gcs as parquet
    upload_results(ctx.bucket_name, ctx.clean_folder, ctx.vehicles_folder, df_final, df_involved)

    print(f"{datetime.now(ZoneInfo('Asia/Manila')).strftime('%Y-%m-%d')} daily processing complete.")


def run_batch_transform(start_date, end_date):
    """Run batch transformation for a specified date range.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.

    This function initializes Spark and GCS, generates the list of raw filenames
    for the requested date range, processes each file, and uploads cleaned parquet
    outputs to GCS.
    """

    ctx = initialize('Transform Stage (Batch)', 'scraped')

    filenames = generate_filenames(ctx.scrape_folder, start_date, end_date)

    for raw_filename in filenames:
        df_final, df_involved = process_single_file(ctx, raw_filename)

        if df_final is None:
            print(f"{raw_filename} does not exist")
            return
        
        # upload to gcs as parquet
        upload_results(ctx.bucket_name, ctx.clean_folder, ctx.vehicles_folder, df_final, df_involved)

    print("Batch processing complete.")


def run_kaggle_transform():
    """Run transformation for Kaggle historical data.

    This function initializes Spark and GCS, processes the Kaggle raw file,
    transforms it, and uploads the cleaned parquet output to GCS.
    """
    ctx = initialize('Transform Stage (Kaggle)', 'kaggle')

    kaggle_folder = f"{ctx.raw_folder}/kaggle"
    raw_filename = f"{kaggle_folder}/kaggle_historical_data.csv"

    df_final, df_involved = process_kaggle_file(ctx, raw_filename)
    
    if df_final is None:
        print(f"{raw_filename} does not exist")
        return
    
    # upload to gcs as parquet
    upload_results(ctx.bucket_name, ctx.clean_folder, ctx.vehicles_folder, df_final, df_involved)

    print("Kaggle processing complete.")


def main():
    """
    Main entry point. Runs scraped data transform by default, or kaggle if
    DATA_SOURCE environment variable is set to 'kaggle'.
    For scraped data, runs in daily mode by default, or batch mode if
    START_DATE and END_DATE environment variables are set.
    """
    data_source = os.getenv("DATA_SOURCE", "scraped")

    if data_source == "kaggle":
        print("Running Kaggle Transform...")
        run_kaggle_transform()
    else:
        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")

        if start_date and end_date:
            print(f"Running Batch Transform from {start_date} to {end_date}...")
            run_batch_transform(start_date, end_date)
        else:
            print("Running Daily Transform...")
            run_daily_transform()


if __name__ == "__main__":
    main()