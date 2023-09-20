
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from configparser import ConfigParser
import boto3
from sql_statements.query import select
from sql_statements.create import raw_data_tables, transformed_tables

from sql_statements import create
from utils.helper import connect_to_dwh  # import dwh connection function
from utils.constants import db_tables
from utils.helper import create_bucket
from sql_statements.transform import transformation_queries
config = ConfigParser()

config.read('.env')

# s3 bucket connection
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

# DWH connection details
DWH_HOST = config['DWH']['dwh_host']
DWH_USER = config['DWH']['dwh_username']
DWH_PASSWORD = config['DWH']['dwh_password']
DWH_DATABASE = config['DWH']['dwh_database']

DWH_IAM_ROLE = config['DWH']['arn_role']


# bucket_name and table_name - path to save our data in s3 bucket data lake
# file name in our bucket, and table name we saving with - saving path
s3_path = 's3://{}/{}.csv'

# Step 1: Create an s3 bucket using boto 3, imported from helper.py
create_bucket()

# Step 2: Extract our tables from the DB to the data lake with sqlaalchemy

conn = create_engine(
    f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

# Extract all tables to csv from DB and first load to s3 bucket


for table in db_tables:
    # query = f'SELECT * FROM {table}'
    df = pd.read_sql_query(select.query.format(table), con=conn)
    df.to_csv(
        s3_path.format(S3_BUCKET_NAME, table), index=False, storage_options={
            'key': ACCESS_KEY, 'secret': SECRET_KEY
        }
    )
# Step 3: create the raw schema in DWH - you first need to connect to dwh
# from the function, it takes conn_details as an arguement, don't forget a dict is key, value so host in quote


conn_details = {
    'host': DWH_HOST,
    'user': DWH_USER,
    'database': DWH_DATABASE,
    'password': DWH_PASSWORD
}
dwh_conn = connect_to_dwh(conn_details)
print('Connected to data warehouse')
cursor = dwh_conn.cursor()
# creating the dev schema
cursor.execute(create.create_dev_schema)

schema = 'raw_data'

#  ---Creating the raw tables in redshift raw schema
for query in raw_data_tables:
    # Just for debugging at every iteration, print first 50 charac
    print(f"============={query[:50]}")
    cursor.execute(query)
    dwh_conn.commit()


# ---copy all the tables from s3 data lake to DWH, using the copy command
# we loop through the db_tables name and run the use the same sql create statemtent from
# since we use the same table name as our query name and same name as our s3 bucket, its easy to loop through.
# s3-path name defines above takes 2 args, so we use .format
# Also from the copy syntax, from and iam role must be in quote
# We also ignore the headers on the csv, delimeter is csv 1 = True

# ---Copying from s3 to redshift
for table in db_tables:
    copy_query = f"""
    copy {schema}.{table} 
    from '{s3_path.format(S3_BUCKET_NAME, table)}' 
    iam_role '{DWH_IAM_ROLE}'
    delimiter ','
    ignoreheader 1;

"""
    cursor.execute(copy_query)
    dwh_conn.commit()

# Step 4 ----Transform from schema to a star schema inside our redshift staging schema
dwh_conn = connect_to_dwh(conn_details)
cursor = dwh_conn.cursor()

# ---create staging schema
cursor.execute(create.staging_schema)
dwh_conn.commit()

# ------ create facts and dimensions tables in staging_schema in redshift
for query in transformed_tables:
    print(f'''------------- {query[:50]}''')
    cursor.execute(query)
    dwh_conn.commit()

# Select insert into the transformed tables
for query in transformation_queries:
    # Just for debugging at every iteration, print first 50 charac
    print(f"============={query[:50]}")
    cursor.execute(query)
    dwh_conn.commit()

cursor.close()
conn.close()
