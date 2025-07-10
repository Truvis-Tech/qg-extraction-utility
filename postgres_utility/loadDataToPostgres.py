import json
import os
import pandas as pd
import numpy as np
from .db_config import get_db_url_and_schema
from datetime import datetime
from google.cloud.sql.connector import Connector
import sqlalchemy

# --- Config ---
instance_conn, database, iam_user, schema, parquet_folder, ip_type = get_db_url_and_schema()

connector = Connector()

def getconn():
    conn = connector.connect(
        instance_conn,
        "pg8000",
        user=iam_user,
        db=database,
        enable_iam_auth=True,
        ip_type=ip_type,
    )
    return conn
engine = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

# --- Function to ensure table name length limit ---
def get_safe_table_name(file_name):
    # Remove extension like .parquet if present
    file_name = file_name.replace('.parquet', '')

    # Split on underscores
    parts = file_name.split('.')
    # print(parts)
    base_name = parts[0]  # Remove any file extension if present
    
    return base_name
    
# tables_names = []
# for _ in (os.listdir(parquet_folder)):
#     tables_names.append(get_safe_table_name(_))

# # print(tables_names)

def serialize_complex_columns(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list, np.ndarray))).any():
            def safe_serialize(x):
                try:
                    if isinstance(x, np.ndarray):
                        return json.dumps(x.tolist())
                    elif isinstance(x, (dict, list)):
                        return json.dumps(x)
                    else:
                        return x
                except (TypeError, ValueError):
                    return None  # or json.dumps(str(x)) if you want to stringify it
            df[col] = df[col].apply(safe_serialize)
    return df


# --- Process All Parquet Files ---
try:
    for file in os.listdir(parquet_folder):
        if file.endswith('.parquet'):
            file_path = os.path.join(parquet_folder, file)
            print(f"Reading {file_path}...")
            
            # Read Parquet file
            df = pd.read_parquet(file_path)
            df = serialize_complex_columns(df)
            table_name = get_safe_table_name(file)

            # Write to PostgreSQL
            df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
            print(f"Loaded data into table: {table_name}")

finally:
    engine.dispose()
    connector.close()