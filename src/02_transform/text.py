import pyspark
from pyspark.sql import SparkSession

gcs_connector_path = './gcs-connector-hadoop3-latest.jar'

spark = SparkSession.builder \
        .master("local[*]") \
        .appName('Read From GCS Bucket') \
        .config("spark.jars", gcs_connector_path) \
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0') \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate() 