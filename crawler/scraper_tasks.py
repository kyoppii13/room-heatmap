import lxml.html
import boto3
import os
import json
import logging
import botocore

logging.basicConfig(level=logging.INFO)


def lambda_handler(event, context):
    BUCKET_NAME = 'suumo-html'
    DIR_NAME = 'v1'

    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    rents_table = dynamodb.Table('rents')

    message_list = event['Records']
    for message in message_list:
        try:
            message_body = json.loads(message['body'])
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
        except botocore.exceptions.ClientError as e:
            logging.error('client error')
            raise Exception(e)
        except Exception as e:
            logging.error('other error')
            raise Exception(e)


def scrape_detail_page(html: str) -> dict:
    root = lxml.html.fromstring(html)
    chintai = {
        'title': root.cssselect('h1.section_h1-header-title')[0].text_content(),
        'rent': root.cssselect('span.property_view_note-emphasis')[0].text_content()
    }
    return chintai
