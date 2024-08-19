import logging
import urllib.parse
import boto3
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def send_sqs_message(msg_body):
    sqs_client = boto3.client('sqs')
    sqs_queue_url = "<my-queue-url>"
    try:
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url, MessageBody=json.dumps(msg_body))
    except Exception as e:
        logging.error(e)
        return None
    return msg


def execute(local_file_path):
    pass


def lambda_handler(event, context):
    s3_client = boto3.resource('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        local_file_path = f'/tmp/{key}'
        s3_client.Bucket(bucket).download_file(key, local_file_path)
        execute(local_file_path)
        return 'SUCCESS'
    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket}.')
        raise e