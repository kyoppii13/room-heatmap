import time
import re
from typing import Iterator
import logging

import requests
import lxml.html
from pymongo import MongoClient
from scraper_tasks import scrape


def main():
    client = MongoClient('mongodb://root:password@mongo:27017')
    collection = client.scraping.suumo_htmls
    collection.create_index('key', unique=True)

    session = requests.Session()
    # 東京都渋谷区 家賃10万以下
    url = 'https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&pc=30&smk=&po1=25&po2=99&shkr1=03&shkr2=03&shkr3=03&shkr4=03&sc=13113&ta=13&cb=0.0&ct=10.0&et=9999999&mb=0&mt=9999999&cn=9999999&fw2='
    response = session.get(url)
    urls = scrape_list_page(response)
    for url in urls:
        key = extract_key(url)

        suumo_html = collection.find_one({'key': key})
        if not suumo_html:
            time.sleep(1)
            logging.info(f'Ferching {url}')
            response = session.get(url)

            collection.insert_one({
                'url': url,
                'key': key,
                'html': response.content,
            })
            scrape.delay(key)


def scrape_list_page(response: requests.Response) -> Iterator[str]:
    html = lxml.html.fromstring(response.text)
    html.make_links_absolute(response.url)

    for a in html.cssselect('table.cassetteitem_other > tbody > tr > td:nth-child(9) > a'):
        url = a.get('href')
        yield url


def extract_key(url: str) -> str:
    m = re.search(r'/chintai/(.+)/', url)
    return m.group(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
