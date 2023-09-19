from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.item import Item, Field
from scrapy.crawler import CrawlerProcess
from web_crawler.web_crawler.items import WebCrawlerItem

import urllib.parse as urlparse
import pandas as pd
import datetime
import logging
import os


class WebsiteSpider(CrawlSpider):
    name = "web_crawler"
    rules = (Rule(LinkExtractor(), callback='parse_page'),)


    def __init__(self, start_url, place_ids, *args, **kwargs):

        start_url_parsed = urlparse.urlparse(start_url)
        domain = start_url_parsed.netloc
        print('*' * 20,start_url)
        logging.info(start_url)
        self.start_urls = [start_url]
        self.allowed_domains = [domain]
        self.place_ids = place_ids.split(',')
        self.field_names = []
        #column loading
        file_path = os.path.join('/home/airflow/gcs/dags',
                                 r'web_crawler/ini/words.txt')
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if(line in ("spider_closed", "update_time", "place_id", "website_url", "facebook", "instagram", "pinterest", "youtube", "twitter", "domain")):
                    continue
                else:
                    self.field_names.append(line)

        
        super().__init__(*args, **kwargs)

    def parse_page(self, response):
        #
        items = WebCrawlerItem()
        url = response.url
        contenttype = response.headers.get("content-type", "").decode('utf-8').lower()
        data = response.body.decode('utf-8')
        data = data.lower()

        items['place_ids'] = []
        items['place_ids'] = self.place_ids

        items['base_url'] = []
        items['base_url'] = url

        items['field_names'] = []
        items['field_names'] = self.field_names

        items['url_content'] = data
        
    

        items['facebook'] = response.xpath('//a[contains(@href, "facebook.com")]/@href').extract()
        items['instagram'] = response.xpath('//a[contains(@href, "instagram.com")]/@href').extract()
        items['pinterest'] = response.xpath('//a[contains(@href, "pinterest.com")]/@href').extract()
        items['youtube'] = response.xpath('//a[contains(@href, "youtube.com")]/@href').extract()
        items['twitter'] = response.xpath('//a[contains(@href, "twitter.com")]/@href').extract()
 
        yield items

        


