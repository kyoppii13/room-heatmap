from typing import Iterator
import logging
import time

import requests
import lxml.html
import boto3
import uuid
import sys

logging.basicConfig(level=logging.INFO)


def main():
    logging.info('Crawling urls')

    session = requests.Session()
    try:
        start_url = sys.argv[1]
    except:
        logging.error('Not enough arguments')
        return

    response = session.get(start_url)
    last_page = get_last_page(response)
    sqs = boto3.resource('sqs')
    queue = sqs.create_queue(QueueName='download')
    for page in range(1, last_page + 1):
        url = f'{start_url}&page={page}'
        logging.info(f'Fetching list page: {url}')
        response = session.get(url)
        urls = scrape_list_page(response)
        logging.info(urls)
        messages = [{'Id': str(uuid.uuid4()), 'MessageBody': url}
                    for url in urls]
        # division 10
        division_per_count = 10
        divided_messages = [messages[i:i + division_per_count]
                            for i in range(0, len(messages), division_per_count)]
        for data in divided_messages:
            response = queue.send_messages(Entries=data)
            logging.info(response)
        time.sleep(1)


def scrape_list_page(response: requests.Response) -> Iterator[str]:
    html = lxml.html.fromstring(response.text)
    html.make_links_absolute(response.url)

    for a in html.cssselect('table.cassetteitem_other > tbody > tr > td:nth-child(9) > a'):
        url = a.get('href')
        yield url


def get_last_page(response: requests.Response) -> int:
    html = lxml.html.fromstring(response.text)
    last_page_str = html.cssselect(
        'ol.pagination-parts > li:last-child > a')[0].text_content()
    logging.info(f'last page: {last_page_str}')
    return int(last_page_str)


if __name__ == '__main__':
    main()
