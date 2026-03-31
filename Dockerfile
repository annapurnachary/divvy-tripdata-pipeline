# Use the official dbt-bigquery image
FROM ghcr.io/dbt-labs/dbt-bigquery:1.7.latest

# Set working directory
WORKDIR /usr/app

# Install Python requirements for your ingestion script
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 1. Copy the dbt configuration files
COPY dbt_project.yml ./
COPY packages.yml* ./ 

# 2. Copy the dbt component folders
COPY models/ ./models/
COPY macros/ ./macros/
COPY seeds/ ./seeds/

# 3. Copy your ingestion script
COPY data_ingestion/Divvy_web_to_gcs.py .

# Set dbt profiles directory to current app dir
ENV DBT_PROFILES_DIR=.
