from google.cloud import storage

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.chunk_size = 5 * 1024 * 1024 # every 5mb upload
    blob.upload_from_filename(local_file)