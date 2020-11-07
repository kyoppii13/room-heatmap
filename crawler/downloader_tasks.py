import re

import logging
from pyqs import task
import requests
from scraper_tasks import scrape
import boto3
import time
import botocore

# logging.basicConfig(level=logging.INFO)


def download():
    BUCKET_NAME = 'suumo-html'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)

    sqs = boto3.resource('sqs')
    download_queue = sqs.get_queue_by_name(QueueName='download')
    scrape_queue = sqs.get_queue_by_name(QueueName='scrape')

    while True:
        message_list = download_queue.receive_messages(MaxNumberOfMessages=10)
        if message_list:
            for message in message_list:
                url = message.body
                key = extract_key(url)
                logging.info(f'Fetching detail page: {url}')

                response = requests.get(url)

                try:
                    bucket.put_object(
                        Key='v1/'+key,
                        Body=response.content,
                    )
                    response = scrape_queue.send_message(MessageBody=key)
                    logging.info(response)
                    message.delete()
                except botocore.exceptions.ClientError as e:
                    logging.error('client error')
                    raise Exception(e)
                except Exception as e:
                    logging.error('other error')
                    raise Exception(e)
        else:
            break


def extract_key(url: str) -> str:
    m = re.search(r'/chintai/(.+)/', url)
    return m.group(1)


if __name__ == '__main__':
    download()
