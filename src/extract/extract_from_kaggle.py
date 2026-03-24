import os
import kagglehub
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

def download_dataset(handle, kaggle_filename, output_dir):
    kagglehub.dataset_download(handle=handle, output_dir=output_dir)

    print(f"{output_dir}/{kaggle_filename} finished downloading!")


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.chunk_size = 5 * 1024 * 1024 # every 5mb upload
    blob.upload_from_filename(local_file)

def main():
    handle = "esparko/mmda-traffic-incident-data"
    output_dir = './.data'
    kaggle_filename = "data_mmda_traffic_spatial.csv"

    bucket = os.environ.get("BUCKET_NAME")
    bucket_folder_name = os.environ.get("FOLDER_NAME")
    bucket_filename = "kaggle_historical_data.csv"

    gcs_object_name = f"{bucket_folder_name}/{bucket_filename}"
    local_file = f"{output_dir}/{kaggle_filename}"

    download_dataset(handle, kaggle_filename, output_dir)

    upload_to_gcs(bucket, gcs_object_name, local_file)
    print(f"{gcs_object_name} finished uploading.")

if __name__ == "__main__":
    main()