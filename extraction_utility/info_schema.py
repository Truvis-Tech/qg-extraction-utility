import os
import argparse
import logging
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import yaml
import re
import traceback
import configparser
import argparse
import pandas as pd

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
    """Get BigQuery project, region, dataset(s), output_dir information from config"""
    config_path = load_config()
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        section = 'extraction_utility'
        project_id = config.get(section, 'project_id')
        region = config.get(section, 'region')
        dataset_id = config.get(section, 'dataset_id').strip()
        service_account_file = config.get(section, 'service_account_file')
        output_dir = config.get(section, 'data_output_directory')

        credentials = service_account.Credentials.from_service_account_file(service_account_file)
        client = bigquery.Client(credentials=credentials, project=project_id)

        # If dataset_id is empty, fetch all datasets in the project
        if not dataset_id:
            datasets = [d.dataset_id for d in client.list_datasets(project=project_id)]
        else:
            # Split by comma and strip whitespace
            datasets = [d.strip() for d in dataset_id.split(',') if d.strip()]

        return client, project_id, region, datasets, output_dir
    except configparser.NoOptionError as e:
        raise ValueError(f"Missing configuration option: {e}")

    # """Set up command-line arguments"""
    # parser = argparse.ArgumentParser(description="Convert BigQuery schema information to parquet")
    # parser.add_argument("--project", required=True, help="Google Cloud Project ID")
    # parser.add_argument("--region", default="region-asia-south1", help="Google Cloud Region")
    # parser.add_argument("--dataset", required=True, help="BigQuery dataset to extract schema from")
    # # parser.add_argument("--schema_type", default="TABLES", choices=["TABLES", "COLUMNS", "VIEWS", "ROUTINES"],
    # #                     help="INFORMATION_SCHEMA view to query")
    # parser.add_argument("--output_dir", default="./Query_output", help="Directory to save the parquet file")
    # parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    # return parser.parse_args()


def setup_logging(verbose):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=level)


def load_queries(filepath):
    try:
        with open(filepath, 'r') as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as e:
        logging.error(f"Error loading queries from {filepath}: {e}")
        raise ValueError(f"Invalid YAML file: {filepath}")


def run_query(client, query):
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        logging.error(f"Error running query: {e}")
        raise


def save_to_parquet(df, output_dir, project_id, region, dataset, name):
    """Save the dataframe to a parquet file"""
    # Build nested output directory
    try:
        dataset_path = output_dir
        os.makedirs(dataset_path, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}.{timestamp}.parquet"
        filepath = os.path.join(dataset_path, filename)

        # Save dataframe to parquet with compression
        df.to_parquet(filepath, index=False, compression='snappy')
        return filepath
    except Exception as e:
        logging.error(f"Error saving to parquet: {e}")
        raise


def extract_table_name(sql_text):
    try:
        match = re.search(r"FROM\s+`?(?:[\w-]+\.){1,2}([\w-]+)`?", sql_text, re.IGNORECASE)
        return match.group(1).lstrip("__") if match else "unknown_table_name"
    except Exception as e:
        logging.error(f"Error extracting table name from SQL: {e}")
        raise


def main():
    client, project_id, region, datasets, output_dir = get_creds()
    setup_logging(verbose=True)

    logging.info(f"Fetching schema information from {project_id} datasets: {datasets} ...")
    queries = load_queries(os.path.abspath(os.path.join(os.path.dirname(__file__),"queries.yaml")))

    # if args.schema_type not in queries:
    #     logging.error(f"No query found for schema_type: {args.schema_type}")
    #     return

    # query = queries[args.schema_type]
    for name, query in queries.items():
        try:
            # Only run TABLES__ logic for query6
            if name == "query6":
                all_tables_df = []
                for dataset_id in datasets:
                    print(f"Processing dataset: {dataset_id}")
                    formatted_query = query.format(region=region, dataset=dataset_id, project_id=project_id)
                    print(f"Query for {dataset_id}: {formatted_query}")
                    df = run_query(client, formatted_query)
                    print(f"Rows returned for {dataset_id}: {len(df)}")
                    all_tables_df.append(df)
                if all_tables_df:
                    combined_df = pd.concat(all_tables_df, ignore_index=True)
                    print(f"Total rows after concat: {len(combined_df)}")
                    filepath = save_to_parquet(combined_df, output_dir, project_id, region, 'ALL_DATASETS', table_name)
                    logging.info(f"Successfully saved {len(combined_df)} rows of schema information to:")
                    logging.info(filepath)
                    logging.info(f"File size: {os.path.getsize(filepath) / (1024 * 1024):.2f} MB")
                else:
                    logging.warning("No __TABLES__ data found for any dataset.")
            else:
                dataset_id = datasets[0]
                formatted_query = query.format(region=region, dataset=dataset_id, project_id=project_id)
                table_name = extract_table_name(formatted_query)
                df = run_query(client, formatted_query)
                filepath = save_to_parquet(df, output_dir, project_id, region, dataset_id, table_name)
                logging.info(f"Successfully saved {len(df)} rows of schema information to:")
                logging.info(filepath)
                logging.info(f"File size: {os.path.getsize(filepath) / (1024 * 1024):.2f} MB")
        except Exception as e:
            logging.error(f"Error: {str(e)}")
            traceback.print_exc()


if __name__ == "__main__":
    main()
