import os
from google.cloud.sql.connector import Connector
import pg8000
from .db_config import get_db_url_and_schema, get_instance_id
import argparse
import re
import pkg_resources

# Establish a connection to Cloud SQL PostgreSQL using IAM authentication
def get_db_connection():
    try:
        instance_connection_name, database, iam_user, schema, input_dir, ip_type = get_db_url_and_schema()
        
        # Initialize Cloud SQL Python Connector
        connector = Connector()
        
        # Get connection using IAM authentication
        conn = connector.connect(
            instance_connection_name,
            "pg8000",
            user=iam_user,
            db=database,
            enable_iam_auth=True,
            ip_type=ip_type,  # Use configured IP type (private/public)
        )
        return conn, connector
    except Exception as e:
        print(f"Error: Unable to connect to the database. {e}")
        return None, None

# Read and execute SQL file
def execute_sql_file(cursor, file_path, schema, instance_id):
    with open(file_path, 'r') as file:
        sql_query = file.read()
        
    sql_query = sql_query.format(schema=schema, instance=instance_id) 
    
    try:
        cursor.execute(sql_query)
        print(f"Successfully executed: {file_path}")
    except Exception as e:
        print(f"Error executing {file_path}: {e}")

def get_ordered_sql_files(folder_path):
    """Get SQL files from folder: number-prefixed files first (numeric sort), then others (alphabetical)"""
    try:
        sql_files = [f for f in os.listdir(folder_path) if f.endswith(".sql")]
        if not sql_files:
            print(f"No SQL files found in {folder_path}")
            return []

        # Files starting with a number
        numbered_files = [f for f in sql_files if f[0].isdigit()]
        non_numbered_files = [f for f in sql_files if not f[0].isdigit()]

        def extract_number(filename):
            match = re.match(r"(\d+)", filename)
            return int(match.group(1)) if match else float('inf')

        ordered_numbered = sorted(numbered_files, key=extract_number)
        ordered_non_numbered = sorted(non_numbered_files)
        ordered_files = ordered_numbered + ordered_non_numbered

        print(f"Processing SQL files in order: {ordered_files}")
        return ordered_files
    except Exception as e:
        print(f"Error getting SQL files from {folder_path}: {e}")
        return []

def process_sql_files(folder_path, schema, instance_id):
    conn, connector = get_db_connection()
    if conn is None:
        return
    
    cursor = conn.cursor()

    try:
        sql_files = get_ordered_sql_files(folder_path)
        
        if not sql_files:
            print("No SQL files to process.")
            return
        
        for filename in sql_files:
            file_path = os.path.join(folder_path, filename)
            print(f"Executing script: {filename}")
            execute_sql_file(cursor, file_path, schema, instance_id)
        
        conn.commit()
        print(f"Successfully executed {len(sql_files)} SQL files")
        
    except Exception as e:
        print(f"Error processing SQL files: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        # Close the connector
        if connector:
            connector.close()

def get_script_directory():
    """Get the directory where the script is located (works in wheel packages)"""
    try:
        package_dir = pkg_resources.resource_filename('trulens-data-transformer', '')
        return package_dir
    except:
        return os.path.dirname(os.path.abspath(__file__))

def get_sql_directories(base_path):
    """Get SQL directories relative to the base path"""
    return {
        'input_sql_dir': os.path.join(base_path, 'unified_data_load_scripts'),
        'output_sql_dir1': os.path.join(base_path, 'recommendation_data_load_scripts'),
        'output_sql_dir2': os.path.join(base_path, 'recommendation_data_load_scripts_2'),
        'output_sql_dir3': os.path.join(base_path, 'recommendation_summary_dl_scripts')
    }

def main():
    parser = argparse.ArgumentParser(description="Run SQL files for input or output data transformation.")
    parser.add_argument("mode", choices=["input", "output"], help="Mode: 'input' or 'output'")
    parser.add_argument("output_folder", nargs="?", help="Path to the output folder (required for output mode)")
    parser.add_argument("--config-path", default=None, help="Path to config file")
    args = parser.parse_args()

    if args.config_path:
        os.environ["CONFIG_PATH"] = args.config_path
    
    instance_connection_name, database, iam_user, schema, input_dir, ip_type = get_db_url_and_schema()
    instance_id = get_instance_id()

    base_path = os.path.dirname(os.path.abspath(__file__))
    sql_dirs = get_sql_directories(base_path)

    if args.mode == "input":
        input_sql_dir = sql_dirs['input_sql_dir']
        if not os.path.exists(input_sql_dir):
            exit(1)
        process_sql_files(input_sql_dir, schema, instance_id)
    elif args.mode == "output":
        output_sql_dir1 = sql_dirs['output_sql_dir1']
        output_sql_dir2 = sql_dirs['output_sql_dir2']
        output_sql_dir3 = sql_dirs['output_sql_dir3']
        for sql_dir in [output_sql_dir1, output_sql_dir2, output_sql_dir3]:
            if not os.path.exists(sql_dir):
                continue
            process_sql_files(sql_dir, schema, instance_id)

if __name__ == "__main__":
    main()