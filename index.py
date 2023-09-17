import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from configparser import ConfigParser
import boto3
from sql import select
from utils.helper import create_bucket

config = ConfigParser()

config.read('.env')

# AWS connection
ACCESS_KEY = config['AWS']['access_key']
SECRET_KEY = config['AWS']['secret_key']
S3_BUCKET_NAME = config['AWS']['bucket_name']
REGION = config['AWS']['region']

# Database connection

HOST = config['DB_CRED']['host']
USERNAME = config['DB_CRED']['user']
PASSWORD = config['DB_CRED']['password']
DATABASE = config['DB_CRED']['database']
PORT = config['DB_CRED']['port']


# bucket_name and table_name - path to save our data in s3 bucket data lake

s3_path = 's3://{}/{}.csv'  # file name in our bucket

# Step 1: Create an s3 bucket using boto 3, imported from helper.py

create_bucket()

# Step 2: Extract our tables from the DB to the data lake

conn = create_engine(
    f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

# Extract all tables to csv from DB and first load to s3 bucket

db_tables = ['banks', 'cards', 'cust_verification_status',
             'transaction_status', 'transactions', 'users', 'wallets']
for table in db_tables:
    # query = f'SELECT * FROM {table}'
    df = pd.read_sql(select.query.format(table), con=conn)
    df.to_csv(
        s3_path.format(S3_BUCKET_NAME, table), index=False, storage_options={
            'key': ACCESS_KEY, 'secret': SECRET_KEY
        }
    )
