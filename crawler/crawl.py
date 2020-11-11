from typing import Iterator
import logging
import time

import requests
import lxml.html
import boto3
import uuid

logging.basicConfig(level=logging.INFO)


def main():
    logging.info('Crawling urls')

    session = requests.Session()
    # 東京都渋谷区 家賃10万以下
    start_url = 'https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&pc=30&smk=&po1=25&po2=99&shkr1=03&shkr2=03&shkr3=03&shkr4=03&sc=13113&ta=13&cb=0.0&ct=10.0&et=9999999&mb=0&mt=9999999&cn=9999999&fw2='

    response = session.get(start_url)
    last_page = get_last_page(response)
    sqs = boto3.resource('sqs')
    queue = sqs.create_queue(QueueName='download')
    for page in range(1, 10):
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
