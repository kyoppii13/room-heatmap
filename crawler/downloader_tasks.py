import re

import logging
from pyqs import task
import requests
from scraper_tasks import scrape
import boto3
import time

logging.basicConfig(level=logging.INFO)
# @task(queue='download')


def download():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('suumo_htmls')

    sqs = boto3.resource('sqs')
    download_queue = sqs.get_queue_by_name(QueueName='download')
    scrape_queue = sqs.get_queue_by_name(QueueName='scrape')

    while True:
        message_list = download_queue.receive_messages(MaxNumberOfMessages=10)
        if message_list:
            for message in message_list:
                url = message.body
                key = extract_key(url)
                suumo_html = table.get_item(Key={'key': key})
                if 'Item' not in suumo_html:
                    logging.info(f'Fetching detail page: {url}')
                    response = requests.get(url)
                    import pdb
                    pdb.set_trace()

                    table.put_item(
                        Item={
                            'url': url,
                            'key': key,
                            'html': response.content,
                        }
                    )
                    response = scrape_queue.send_message(MessageBody=key)
                    logging.info(response)
                else:
                    logging.info(f'already fetched page: {url}')
                message.delete()
        else:
            break


def extract_key(url: str) -> str:
    m = re.search(r'/chintai/(.+)/', url)
    return m.group(1)


if __name__ == '__main__':
    download()
