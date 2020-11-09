from pyqs import task
import lxml.html
import boto3
import os
import json
import logging

logging.basicConfig(level=logging.INFO)


def scrape():
    BUCKET_NAME = 'suumo-html'
    DIR_NAME = 'v1'
    s3 = boto3.client('s3')

    dynamodb = boto3.resource('dynamodb')
    suumo_htmls_table = dynamodb.Table('suumo_htmls')
    rents_table = dynamodb.Table('rents')

    sqs = boto3.resource('sqs')
    scrape_queue = sqs.get_queue_by_name(QueueName='scrape')

    while True:
        message_list = scrape_queue.receive_messages(MaxNumberOfMessages=10)
        if message_list:
            for message in message_list:
                message_body = json.loads(message.body)
                key = message_body['key']
                url = message_body['url']
                response = s3.get_object(
                    Bucket=BUCKET_NAME, Key=os.path.join(DIR_NAME, key))
                binary_html = response['Body'].read()
                logging.info(f'scrapping: {url}')
                suumo = scrape_detail_page(binary_html)

                rents_table.put_item(
                    Item={
                        'key': key,
                        'url': url,
                        **suumo
                    }
                )
                message.delete()


def scrape_detail_page(html: str) -> dict:
    root = lxml.html.fromstring(html)
    chintai = {
        'title': root.cssselect('h1.section_h1-header-title')[0].text_content(),
        'rent': root.cssselect('span.property_view_note-emphasis')[0].text_content()
    }
    return chintai


if __name__ == "__main__":
    scrape()
