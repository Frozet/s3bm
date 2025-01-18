import boto3
from botocore.exceptions import NoCredentialsError
from fastapi import FastAPI, UploadFile, HTTPException

# Настройки для S3
with open('access_data.txt', 'r') as f:
        S3_BUCKET_NAME = f.readline()
        S3_REGION_NAME = f.readline()
        S3_ENDPOINT_URL = f.readline()
        S3_ACCESS_KEY = f.readline()
        S3_SECRET_KEY = f.readline()

# Инициализация клиента S3
s3_client = boto3.client(
    "s3",
    region_name=S3_REGION_NAME,
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

app = FastAPI()