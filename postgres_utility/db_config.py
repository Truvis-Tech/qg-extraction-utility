import configparser
import urllib.parse
from google.cloud.sql.connector import Connector
import os
import sqlalchemy
import argparse
import sys

def resolve_config_path():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-path', default=None)
    args, _ = parser.parse_known_args()  

    env_config_path = os.environ.get('CONFIG_PATH')
    default_path = os.path.abspath('./data_transformation/config/config.ini')

    config_path = args.config_path or env_config_path or default_path
    if config_path and not os.path.exists(config_path):
        config_path = default_path

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    return config_path

def get_db_url_and_schema():
    config = configparser.ConfigParser()
    config_path = resolve_config_path()
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")
    config.read(config_path)
    
    section = 'postgres_db'
    project_id = config.get(section, 'project_id')
    region = config.get(section, 'region')
    instance_name = config.get(section, 'instance_name')
    database = config.get(section, 'database')
    iam_user = config.get(section, 'iam_user')
    schema = config.get(section, 'schema')
    input_dir = config.get('extraction_utility', 'data_output_directory')
    ip_type = "private"
    
    instance_connection_name = f"{project_id}:{region}:{instance_name}"
    return instance_connection_name, database, iam_user, schema, input_dir, ip_type

def get_instance_id():
    config = configparser.ConfigParser()
    config_path = resolve_config_path()
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")
    config.read(config_path)
    
    section = 'postgres_db'
    instance_id = config.get(section, 'instance_name')
    
    return instance_id
