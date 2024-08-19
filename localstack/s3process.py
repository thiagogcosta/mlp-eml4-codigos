import os
import boto3
import pandas

# DEFINA O AWS_ACCESS_KEY_ID e o AWS_SECRET_ACCESS_KEY no site...
os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""

URL_ENDPOINT = "https://localhost.localstack.cloud:4566"

s3 = boto3.client("s3", endpoint_url = URL_ENDPOINT, region_name="us-east-1")

path_local = "/home/thiago-costa/Documentos/mba_machine_learning_in_production/eml4/localstack"

response = s3.get_object(Bucket="data-lake-local", Key = "churn_internet.csv")

file = response["Body"]

df = pandas.read_csv(file)

counts = df.groupby("churn").count()
print(counts)
counts.to_csv(f"{path_local}/temp.csv", index=True)

with open(f'{path_local}/temp.csv', 'rb') as file:
    s3.upload_fileobj(file, "data-lake-local", "churn.csv")