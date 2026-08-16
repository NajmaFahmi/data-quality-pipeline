# FILE      : jobs/gcs_uploader.py
# LIBRARY   : google-cloud-storage
# CONFIG    : local_path, bucket_name, destination_blob — via function parameters
# __main__  : none
# CALLER    : run_pipeline.py

from google.cloud import storage


def upload_to_bronze(
        local_path: str,
        bucket_name: str,
        destination_blob: str,
) -> str:
    """
    Upload a local file to GCS Bronze layer.

    Args:
        local_path: Path to the local file.
        bucket_name: GCS bucket name.
        destination_blob: Destination path inside the bucket.

    Returns:
        Full GCS URI of the uploaded file.
    """

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    blob.upload_from_filename(local_path)

    gcs_uri = f"gs://{bucket_name}/{destination_blob}"
    print(f"Uploaded {local_path} -> {gcs_uri}")
    return gcs_uri

