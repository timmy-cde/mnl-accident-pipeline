import os
from dotenv import load_dotenv
from upload_to_gcs import upload_to_gcs

load_dotenv()

def main():
    bucket = os.environ.get("BUCKET_NAME")
    bucket_folder_name = os.environ.get("RAW_FOLDER_NAME")
    bucket_filename = "initial_locations.parquet"
    
    local_file = "../00_init_data/initial_locations.parquet"

    gcs_object_name = f"{bucket_folder_name}/locations/{bucket_filename}"

    upload_to_gcs(bucket, gcs_object_name, local_file)
    print(f"{gcs_object_name} was uploaded to gcs.")

if __name__ == "__main__":
    main()