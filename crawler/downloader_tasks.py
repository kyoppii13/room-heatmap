import re
import logging
import requests
import boto3
import botocore
import json

logging.basicConfig(level=logging.INFO)


def lambda_handler(event, context):
    BUCKET_NAME = 'suumo-html'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)

    sqs = boto3.resource('sqs')
    scrape_queue = sqs.get_queue_by_name(QueueName='scrape')

    message_list = event['Records']
    for message in message_list:
        url = message['body']
        key = extract_key(url)
        logging.info(f'Fetching detail page: {url}')

        response = requests.get(url)

        try:
            bucket.put_object(
                Key='v1/' + key,
                Body=response.content,
            )
            response = scrape_queue.send_message(
                MessageBody=json.dumps({"key": key, "url": url}))
            logging.info(response)
        except botocore.exceptions.ClientError as e:
            logging.error('client error')
            raise Exception(e)


def extract_key(url: str) -> str:
    m = re.search(r'/chintai/(.+)/', url)
    return m.group(1)
