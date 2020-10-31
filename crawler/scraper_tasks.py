import re

from pyqs import task
import lxml.html
from pymongo import MongoClient


@task(queue='scrape')
def scrape(key: str):
    client = MongoClient('mongodb://root:password@mongo:27017')
    html_collection = client.scraping.suumo_htmls

    suumo_html = html_collection.find_one({'key': key})
    suumo = scrape_detail_page(key, suumo_html['url'], suumo_html['html'])

    chintai_collection = client.scraping.chintai
    chintai_collection.create_index('key', unique=True)
    chintai_collection.update_one({'key': key}, {'$set': suumo}, upsert=True)


def scrape_detail_page(key: str, url: str, html: str) -> dict:
    root = lxml.html.fromstring(html)
    chintai = {
        'url': url,
        'key': key,
        'title': root.cssselect('h1.section_h1-header-title')[0].text_content(),
        'rent': root.cssselect('span.property_view_note-emphasis')[0].text_content()
    }
    return chintai
