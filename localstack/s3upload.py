import os
import boto3

# DEFINA O AWS_ACCESS_KEY_ID e o AWS_SECRET_ACCESS_KEY no site...
os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""

URL_ENDPOINT = "https://localhost.localstack.cloud:4566"

s3 = boto3.client("s3", endpoint_url = URL_ENDPOINT, region_name="us-east-1")

path_local = "/home/thiago-costa/Documentos/mba_machine_learning_in_production/eml4/localstack"
filename = "churn_internet.csv"

with open(f'{path_local}/{filename}', 'rb') as file:
    s3.upload_fileobj(file, "data-lake-local", filename)