import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

from s3pipeline import Page
from myproject.items import Room


class SuumoSpider(CrawlSpider):
    name = 'suumo'
    allowed_domains = ['suumo.jp']
    # 東京都全エリア
    start_urls = ['https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13101&sc=13102&sc=13103&sc=13104&sc=13105&sc=13113&sc=13106&sc=13107&sc=13108&sc=13118&sc=13121&sc=13122&sc=13123&sc=13109&sc=13110&sc=13111&sc=13112&sc=13114&sc=13115&sc=13120&sc=13116&sc=13117&sc=13119&sc=13201&sc=13202&sc=13203&sc=13204&sc=13205&sc=13206&sc=13207&sc=13208&sc=13209&sc=13210&sc=13211&sc=13212&sc=13213&sc=13214&sc=13215&sc=13218&sc=13219&sc=13220&sc=13221&sc=13222&sc=13223&sc=13224&sc=13225&sc=13227&sc=13228&sc=13229&sc=13300&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1']

    rules = [
        Rule(LinkExtractor(allow=r'.+&page=\d$')),
        Rule(LinkExtractor(allow=r'/chintai/\w+_\d+'), callback='parse_room')
    ]

    def parse_room(self, response):
        yield Page.from_response(response)
