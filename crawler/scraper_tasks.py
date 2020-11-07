from pyqs import task
import lxml.html
import boto3


@task(queue='scrape')
def scrape(key: str):
    dynamodb = boto3.resource('dynamodb')
    suumo_htmls_table = dynamodb.Table('suumo_htmls')
    rents_table = dynamodb.Table('rents')

    suumo_html = suumo_htmls_table.get_item(Key={'key': key})
    suumo = scrape_detail_page(
        key, suumo_html['Item']['url'], suumo_html['Item']['html'].value)

    rents_table.put_item(
        Item={
            'key': key,
            **suumo
        }
    )


def scrape_detail_page(key: str, url: str, html: str) -> dict:
    root = lxml.html.fromstring(html)
    chintai = {
        'url': url,
        'key': key,
        'title': root.cssselect('h1.section_h1-header-title')[0].text_content(),
        'rent': root.cssselect('span.property_view_note-emphasis')[0].text_content()
    }
    return chintai


if __name__ == "__main__":
    scrape()
