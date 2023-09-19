# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class WebCrawlerItem(scrapy.Item):
    url_content = scrapy.Field()
    place_ids = scrapy.Field()
    field_names = scrapy.Field()
    website_url = scrapy.Field()
    base_url = scrapy.Field()

    facebook = scrapy.Field()
    instagram = scrapy.Field()
    pinterest = scrapy.Field()
    youtube = scrapy.Field()
    twitter = scrapy.Field()
