import os
import io
import requests
import zipfile
import pandas as pd
from google.cloud import storage

# --- Parameters from Kestra ---
YEAR = os.environ.get("YEAR")
MONTH = os.environ.get("MONTH")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

BASE_URL = "https://divvy-tripdata.s3.amazonaws.com/" 

def divvy_to_gcs(year, month):
    # Format: YYYYMM-divvy-tripdata.zipmkd
    file_prefix = f"{year}{int(month):02d}-divvy-tripdata"
    zip_file_name = f"{file_prefix}.zip"
    request_url = f"{BASE_URL}{zip_file_name}"

    print(f"Executing for: {year}-{month}")
    r = requests.get(request_url)
    if r.status_code != 200:
        print(f"Skipping: {zip_file_name} not found.")
        return

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        csv_files = [f for f in z.namelist() if f.endswith('.csv') and not f.startswith('__')]
        for csv_file in csv_files:
            with z.open(csv_file) as f:
                df = pd.read_csv(f)
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
                
                # Upload logic
                client = storage.Client()
                blob = client.bucket(BUCKET).blob(f"raw/divvy/{year}/{int(month):02d}/{csv_file.replace('.csv', '.parquet')}")
                blob.upload_from_file(parquet_buffer, rewind=True)

if __name__ == "__main__":
    if not all([YEAR, MONTH, BUCKET]):
        print("Error: Missing environment variables YEAR, MONTH, or GCP_GCS_BUCKET")
    else:
        divvy_to_gcs(YEAR, MONTH)
