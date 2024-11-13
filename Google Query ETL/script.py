import subprocess
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
import snowflake.connector
from dotenv import load_dotenv

CLIENT = 'OCF'
# Function to install packages
def install_packages():
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
    except Exception as e:
        print(f"Failed to install packages from requirements.txt: {e}")
        sys.exit(1)
install_packages()

load_dotenv()

# Constants and credentials
SITE_URL = os.getenv("SITE_URL")
BUCKET = os.getenv("BUCKET")
FOLDER = os.getenv("FOLDER")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")
STAGING_TABLE = os.getenv("STAGING_TABLE")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100000"))

GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

client = 'ocf'
# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='us-east-1'
)

# Connect to Snowflake
def connect_snowflake():
    if not SNOWFLAKE_ACCOUNT or not SNOWFLAKE_USER:
        raise ValueError("Snowflake credentials are missing.")
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )
    print("Connected to Snowflake.")
    return conn

# Authenticate Google Search Console API
def auth_search_console():
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
    )
    return build("searchconsole", "v1", credentials=credentials)

# Fetch data from Google Search Console
def fetch_data_from_gsc(service, start_date, end_date):
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['query']
    }
    response = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
    if 'rows' in response:
        data = [
            {
                'Query': row['keys'][0],
                'Clicks': row.get('clicks', 0),
                'Impressions': row.get('impressions', 0),
                'CTR': row.get('ctr', 0),
                'Position': row.get('position', 0)
            }
            for row in response['rows']
        ]
        df = pd.DataFrame(data)
        df['start_dt'] = start_date
        df['end_dt'] = end_date
        return df
    else:
        print("No data found.")
        return pd.DataFrame()

# Upload data to S3 in batches
def upload_to_s3(df):
    num_batches = len(df) // BATCH_SIZE + 1
    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = min((i + 1) * BATCH_SIZE, len(df))
        batch_data = df[start_idx:end_idx]
        file_name = f'batch_{i+1}.csv'
        batch_data.to_csv(file_name, index=False)
        batch_data['client'] = CLIENT
        s3.upload_file(file_name, BUCKET, f"{FOLDER}/{file_name}")
        os.remove(file_name)
        print(f"Uploaded {file_name} to S3.")

# Load data from S3 to Snowflake
def load_to_snowflake(conn):
    cursor = conn.cursor()
    try:
        copy_command = f"""
        COPY INTO {STAGING_TABLE}
        FROM 's3://{BUCKET}/{FOLDER}/'
        CREDENTIALS = (AWS_KEY_ID='{AWS_ACCESS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}')
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER=1)
        ON_ERROR='CONTINUE';
        """
        cursor.execute(copy_command)
        print("Data copied into staging table.")

        merge_command = f"""
        MERGE INTO {SNOWFLAKE_TABLE} AS tgt
        USING {STAGING_TABLE} AS src
        ON tgt.query = src.query
           AND tgt.start_dt = src.start_dt
           AND tgt.end_dt = src.end_dt
        WHEN MATCHED THEN
            UPDATE SET
                tgt.clicks = src.clicks,
                tgt.impressions = src.impressions,
                tgt.ctr = src.ctr,
                tgt.position = src.position,
                tgt.client = src.client

        WHEN NOT MATCHED THEN
            INSERT (query, clicks, impressions, ctr, position, start_dt, end_dt,client)
            VALUES (src.query, src.clicks, src.impressions, src.ctr, src.position, src.start_dt, src.end_dt,src.client);
        """

        cursor.execute(merge_command)
        cursor.execute(f"truncate {STAGING_TABLE};")

        print("Data merged into target table.")
    finally:
        cursor.close()

# Clear S3 folder
def clear_s3_folder():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=FOLDER)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=BUCKET, Key=obj['Key'])
            print(f"Deleted {obj['Key']} from S3.")
    else:
        print("No objects found to delete.")

# Main ETL Process

def run_etl():
    service = auth_search_console()
    conn = connect_snowflake()
    try:
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        # Fetch, transform, and upload data
        df = fetch_data_from_gsc(service, start_date, end_date)
        df['client'] = client
        df['timestamp'] = ''
        if not df.empty:
            upload_to_s3(df)
            load_to_snowflake(conn)
            clear_s3_folder()
            print("ETL process completed successfully.")
        else:
            print("ETL process completed with no data to process.")
    except Exception as e:
        print(f"ETL process failed: {e}")
    finally:
        conn.close()

# Trigger ETL
run_etl()