import boto3
from configparser import ConfigParser

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
