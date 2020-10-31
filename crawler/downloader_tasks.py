import re

import logging
from pyqs import task
from pymongo import MongoClient
import requests
from scraper_tasks import scrape
import boto3


@task(queue='download')
def download(url: str):
    client = MongoClient('mongodb://root:password@mongo:27017')
    html_collection = client.scraping.suumo_htmls
    html_collection.create_index('key', unique=True)

    key = extract_key(url)
    suumo_html = html_collection.find_one({'key': key})
    if not suumo_html:
        logging.info(f'Ferching detail page: {url}')
        response = requests.get(url)

        html_collection.insert_one({
            'url': url,
            'key': key,
            'html': response.content,
        })

        scrape.delay(key)


def extract_key(url: str) -> str:
    m = re.search(r'/chintai/(.+)/', url)
    return m.group(1)
