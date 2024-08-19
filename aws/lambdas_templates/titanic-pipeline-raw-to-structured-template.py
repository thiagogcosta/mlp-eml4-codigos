import urllib.parse
import boto3


def create_structured_file(json_content):
    pass


def lambda_handler(event, context):
    s3_client = boto3.resource('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        object = s3_client.Object(bucket, key)
        file_content = object.get()['Body'].read()
        create_structured_file(file_content)
        return 'SUCCESS'
    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket}.')
        raise e