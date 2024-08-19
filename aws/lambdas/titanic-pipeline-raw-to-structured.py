import boto3
import csv
import logging
import urllib.parse

from datetime import datetime

logging.getLogger().setLevel(logging.INFO)


def get_file_name(base_name):
    return datetime.now().strftime(f"%Y%m%d%H%M%S-{base_name}")


def create_structured_file(json_content):
    file_name = get_file_name('titanic.csv')
    local_csv_file = f'/tmp/{file_name}'
    data_file = open(local_csv_file, 'w')
    csv_writer = csv.writer(data_file)
    count = 0
    for passenger in json_content:
        if count == 0:
            header = passenger.keys()
            csv_writer.writerow(header)
            count += 1
        csv_writer.writerow(passenger.values())
    data_file.close()

    bucket_name = 'ml-mba-ufscar-titanic-pipeline-structured'

    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(local_csv_file, bucket_name, file_name)


def lambda_handler(event, context):
    s3_client = boto3.resource('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    logging.info(f"Processing file: {key}")
    try:
        object = s3_client.Object(bucket, key)
        file_content = object.get()['Body'].read()
        create_structured_file(file_content)
        return 'SUCCESS'
    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket}.')
        raise e
