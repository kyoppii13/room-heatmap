from typing import Iterator
import logging

import requests
import lxml.html
from downloader_tasks import download


def main():
    logging.info('Crawling urls')

    session = requests.Session()
    # 東京都渋谷区 家賃10万以下
    start_url = 'https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&pc=30&smk=&po1=25&po2=99&shkr1=03&shkr2=03&shkr3=03&shkr4=03&sc=13113&ta=13&cb=0.0&ct=10.0&et=9999999&mb=0&mt=9999999&cn=9999999&fw2='

    response = session.get(start_url)
    last_page = get_last_page(response)
    for page in range(1, last_page + 1):
        url = f'{start_url}&page={page}'
        logging.info(f'Fetching list page: {url}')
        response = session.get(url)
        urls = scrape_list_page(response)
        logging.info(urls)
        for url in urls:
            download.delay(url)


def scrape_list_page(response: requests.Response) -> Iterator[str]:
    html = lxml.html.fromstring(response.text)
    html.make_links_absolute(response.url)

    for a in html.cssselect('table.cassetteitem_other > tbody > tr > td:nth-child(9) > a'):
        url = a.get('href')
        yield url


def get_last_page(response: requests.Response) -> int:
    html = lxml.html.fromstring(response.text)
    last_page_str = html.cssselect('ol.pagination-parts > li:last-child > a')[0].text_content()
    logging.info(f'last page: {last_page_str}')
    return int(last_page_str)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
