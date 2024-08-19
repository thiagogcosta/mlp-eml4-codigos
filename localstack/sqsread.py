import os
import boto3
import time

# DEFINA O AWS_ACCESS_KEY_ID e o AWS_SECRET_ACCESS_KEY no site...
os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""

URL_ENDPOINT = "https://localhost.localstack.cloud:4566"

SQS_URL = "https://localhost.localstack.cloud:4566/000000000000/localstack-queue"

sqs = boto3.client("sqs", endpoint_url = URL_ENDPOINT, region_name="us-east-1")

while True:

    response =sqs.receive_message(
        QueueUrl = SQS_URL,
        MaxNumberOfMessages = 1,
        WaitTimeSeconds = 0
    )

    try:
        message = response["Messages"][0]
        print(message)
        receipt_handle = message["ReceiptHandle"]
        sqs.delete_message(
            QueueUrl = SQS_URL,
            ReceiptHandle = receipt_handle
        )
        print(f"Read and Delete msg {message}")

    except:
        pass
    time.sleep(1)