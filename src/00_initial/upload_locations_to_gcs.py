import os
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

def upload_to_gcs(bucket_name, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.chunk_size = 5 * 1024 * 1024 # every 5mb upload
    blob.upload_from_filename(local_file)

def main():
    bucket = os.environ.get("BUCKET_NAME")
    bucket_folder_name = os.environ.get("RAW_FOLDER_NAME")
    bucket_filename = "initial_locations.parquet"
    
    local_file = "./initial_locations.parquet"

    gcs_object_name = f"{bucket_folder_name}/locations/{bucket_filename}"

    upload_to_gcs(bucket, gcs_object_name, local_file)
    print(f"{gcs_object_name} was uploaded to gcs.")

if __name__ == "__main__":
    main()