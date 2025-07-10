#!/usr/bin/env python3
# BigQuery Logs to Parquet Converter
# This script fetches recent BigQuery logs and converts them to Parquet files

import os
import argparse
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import pandas as pd
from google.oauth2 import service_account
import configparser

# def setup_args():
#     """Set up command line arguments."""
#     parser = argparse.ArgumentParser(description='Convert BigQuery logs to Parquet files')
#     parser.add_argument('--project_id', required=True, help='Your GCP project ID')
#     parser.add_argument("--region", default = "region-asia-south1", help="Google Cloud Region")
#     parser.add_argument('--days', type=int, default=1, help='Number of days of logs to retrieve (default: 7)')
#     parser.add_argument('--output_dir', default='./Logs_output', help='Directory to save Parquet files')
#     parser.add_argument('--log_type', default='resource',
#                         choices=['query', 'job', 'resource', 'all'],
#                         help='Type of logs to extract (default: resource)')
#     return parser.parse_args()

def load_config():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-path', default=None)
    args, _ = parser.parse_known_args()  

    env_config_path = os.environ.get('CONFIG_PATH')
    default_path = os.path.abspath('./extraction_utility/config/config.ini')

    config_path = args.config_path or env_config_path or default_path
    if config_path and not os.path.exists(config_path):
        config_path = default_path

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    return config_path


def get_creds():
    """Get BigQuery project, region, dataset, output_dir information from config"""
    config_path = load_config()
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        section = 'extraction_utility'
        project_id = config.get(section, 'project_id')
        region = config.get(section, 'region')
        dataset_id = config.get(section, 'dataset_id')
        service_account_file = config.get(section, 'service_account_file')

        section = 'extraction_utility_logs'
        output_dir = config.get(section, 'logs_output_directory')
        days = config.get(section, 'days')
        log_type = config.get(section, 'log_type')

        credentials = service_account.Credentials.from_service_account_file(service_account_file)
        client = bigquery.Client(credentials=credentials, project=project_id)

        return client, project_id, region, dataset_id, output_dir, days, log_type
    except configparser.NoOptionError as e:
        raise ValueError(f"Missing configuration option: {e}")


def get_resource_logs_sql(project_id, days, region):
    """Create SQL query to fetch BigQuery resource usage logs for the last `days`."""
    try:
        return f"""
        SELECT *
        FROM
        `{project_id}.{region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` AS a
        WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        """
    except Exception as e:
        print(f"Error creating SQL query: {e}")
        raise

def execute_query(client, query):
    """Execute a BigQuery query and return results as DataFrame."""
    try:
        query_job = client.query(query)
        results = query_job.result()
        return results.to_dataframe()
    except GoogleAPIError as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()

def save_to_parquet(df, output_path,log_type):
    """Save DataFrame to Parquet file."""
    os.makedirs(output_path, exist_ok=True)
    log_path = output_path + "/" + f"{log_type}"
    os.makedirs(log_path, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(log_path, f"{timestamp}.parquet")

    df.to_parquet(file_path, engine='pyarrow', compression='snappy')
    print(f"Saved {df.shape[0]} records to {file_path}")
    return file_path


def main():
    client, project_id, region, dataset_id, output_dir, days, log_type = get_creds()
    log_type = log_type
    if log_type == 'resource':
        log_types_to_process = ['resource']
    else:
        log_types_to_process = [log_type]

    for log_type in log_types_to_process:
        print(f"\nFetching {log_type} logs for the past {days} days...")
        query = get_resource_logs_sql(project_id, days, region)
        df = execute_query(client, query)
        if not df.empty:
            file_path = save_to_parquet(df, output_dir,log_type)
            print(f"Resource logs saved to: {file_path}")
        else:
            print("No resource logs found or error occurred.")

if __name__ == "__main__":
    main()