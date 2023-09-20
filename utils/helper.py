import boto3
from configparser import ConfigParser
import redshift_connector as rdc

# Importing variables from index.py
from index import ACCESS_KEY, REGION, S3_BUCKET_NAME, SECRET_KEY


def create_bucket():
    client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION
    )
# Creating a s3 bucket, we use the same client created above
    client.create_bucket(
        Bucket=S3_BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION
        }
    )

# Create connection to the redshift DWH
# Instead of passing in the conn params, we can pass it in as an argument which takes in a dict


def connect_to_dwh(conn_details):
    return rdc.connect(**conn_details)

# * can take an iterable list as an arg
# ** can take key value pairs which python unpacks as a dictionary
