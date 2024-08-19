import os
import boto3

# DEFINA O AWS_ACCESS_KEY_ID e o AWS_SECRET_ACCESS_KEY no site...
os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""

URL_ENDPOINT = "https://localhost.localstack.cloud:4566"

SQS_URL = "https://localhost.localstack.cloud:4566/000000000000/localstack-queue"

sqs = boto3.client("sqs", endpoint_url = URL_ENDPOINT, region_name="us-east-1")

for i in range(10):
    response = sqs.send_message(
        QueueUrl = SQS_URL,
        DelaySeconds = 1,
        MessageAttributes = {
            "Titulo": {
                "DataType": "String",
                "StringValue": f"Aula - {i}"
            }
        },
        MessageBody=(
            f"Teste - {i}"
        )
    )
    print(response["MessageId"])