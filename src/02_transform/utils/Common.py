import pyspark.sql.functions as F
# from utils.PostParser import post_parser
from utils.PostParserRefactored import post_parser
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

def gcs_file_read(spark, bucket_name, filename, schema):
    df = spark.read \
        .option("header", True) \
        .option("multiline", True) \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("ignoreLeadingWhiteSpace", True) \
        .option("ignoreTrailingWhiteSpace", True) \
        .schema(schema) \
        .csv(f"gs://{bucket_name}/{filename}")
    
    return df

def gcs_upload_parquet(bucket_name, clean_folder, df):
    df.write \
        .mode("append") \
        .partitionBy("date") \
        .parquet(f"gs://{bucket_name}/{clean_folder}/")
    

def partial_parse_raw_data(df_raw):

    PartialPostSchema = StructType([
        StructField('date', DateType(), True),
        StructField('time', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('direction', StringType(), True),
        StructField('type', StringType(), True),
        StructField('lanes_blocked', IntegerType(), True),
        StructField('involved', StringType(), True),
        StructField('post', StringType(), True),
        StructField('link', StringType(), True),
    ])
        
    post_parser_udf = F.udf(post_parser, PartialPostSchema)

    df_temp = df_raw.withColumn("parsed", 
                                post_parser_udf(
                                    F.upper(df_raw['content']),
                                    df_raw['created_at'],
                                    df_raw['tweetlinkid']
                                    )
                                )
    
    return df_temp.select(
        F.col("parsed.date").alias("date"),
        F.col("parsed.time").alias("time"),
        F.col("parsed.timestamp").alias("event_timestamp"),
        F.col("parsed.location").alias("location"),
        F.col("parsed.direction").alias("direction"),
        F.col("parsed.type").alias("type"),
        F.col("parsed.lanes_blocked").alias("lanes_blocked"),
        F.col("parsed.involved").alias("involved"),
        F.col("parsed.post").alias("post"),
        F.col("parsed.link").alias("link")
    )