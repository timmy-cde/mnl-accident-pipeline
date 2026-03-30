import os
import kagglehub
from dotenv import load_dotenv
from upload_to_gcs import upload_to_gcs

load_dotenv()

def download_dataset(handle, kaggle_filename, output_dir):
    kagglehub.dataset_download(handle=handle, output_dir=output_dir)

    print(f"{output_dir}/{kaggle_filename} finished downloading!")

def main():
    handle = "esparko/mmda-traffic-incident-data"
    output_dir = './.data'
    kaggle_filename = "data_mmda_traffic_spatial.csv"

    bucket_name = os.environ.get("BUCKET_NAME")
    bucket_folder_name = os.environ.get("RAW_FOLDER_NAME")
    bucket_filename = "kaggle_historical_data.csv"

    gcs_object_name = f"{bucket_folder_name}/kaggle/{bucket_filename}"
    local_file = f"{output_dir}/{kaggle_filename}"

    download_dataset(handle, kaggle_filename, output_dir)

    upload_to_gcs(bucket_name, gcs_object_name, local_file)
    print(f"{gcs_object_name} finished uploading.")

if __name__ == "__main__":
    main()