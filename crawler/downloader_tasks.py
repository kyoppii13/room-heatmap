import re

import logging
from pyqs import task
import requests
from scraper_tasks import scrape
import boto3


@task(queue='download')
def download(url: str):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('suumo_htmls')

    key = extract_key(url)
    try:
        suumo_html = table.get_item(Key={'key': key})
        suumo_html['Item']

    except KeyError as e:
        logging.info(f'Fetching detail page: {url}')
        response = requests.get(url)

        table.put_item(
            Item={
                'url': url,
                'key': key,
                'html': response.content,
            }
        )
        scrape.delay(key)
    else:
        logging.info(f'already fetched page: {url}')


def extract_key(url: str) -> str:
    m = re.search(r'/chintai/(.+)/', url)
    return m.group(1)
