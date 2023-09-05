import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account


# Setup configuration path AND target path.
root_path = os.getcwd()
config_path = f"{root_path}\env_conf"
DATA_FOLDER = f"{root_path}\data"

project_id = "greenery-398007"
bucket_name = "greenery-bucket"
location = "asia-southeast1"
BUSINESS_DOMAIN = "greenery"

# configuration for GCS
keyfile_gcs = f"{config_path}\greenery-google-cloud-storage.json"
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)
storage_client = storage.Client(credentials = credentials_gcs)

# configuration for BigQuery
# permission for IAM service_account ref: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv 
keyfile_bq = f"{config_path}\greenery-398007-load-data-from-gcs-into-bigquery.json"
service_account_info_bq = json.load(open(keyfile_bq))
credentials_bq = service_account.Credentials.from_service_account_info(service_account_info_bq)
bigquery_client = bigquery.Client(credentials = credentials_bq, location = location)


# load data from gcs to bigquery ref : https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client#google_cloud_bigquery_client_Client_load_table_from_uri
# https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.WriteDisposition
def load_data_from_gcs_to_bigquery(file_name):

    table_name= file_name.split(".csv")[0]
    destination_blob_name = f"{BUSINESS_DOMAIN}/{table_name}/{table_name}.csv"
    table_id = f"{project_id}.{BUSINESS_DOMAIN}.{table_name}"

    # gs://greenery-bucket/greenery/addresses/addresses.csv
    uri = f"gs://{bucket_name}/{destination_blob_name}"
    job_config = bigquery.LoadJobConfig(
                    skip_leading_rows = 1,
                    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
                    source_format = bigquery.SourceFormat.CSV,
                    autodetect = True,
                    )

    load_job = bigquery_client.load_table_from_uri(
                                        uri,
                                        table_id,
                                        job_config = job_config,
                                        location = location,
                                        )

    load_job.result()  # Waits for the job to complete.

    destination_table = bigquery_client.get_table(table_id)  # Make an API request.
    print(f"GoogleCloudStorage ---> Bigquery: Loaded {destination_table.num_rows} rows and {len(destination_table.schema)} columns to {table_id}")


if __name__ == "__main__":
    print(os.listdir(DATA_FOLDER))
    greenery_file_list = list(os.listdir(DATA_FOLDER))
    
    for table_name in greenery_file_list:
        load_data_from_gcs_to_bigquery(table_name)
