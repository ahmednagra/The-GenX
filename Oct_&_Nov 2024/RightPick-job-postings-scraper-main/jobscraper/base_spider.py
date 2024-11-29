import scrapy
from scrapy import signals
from datetime import datetime

def spider_opened(spider):
    spider.settings.set('LOG_ENABLED', True)
    spider.settings.set('LOG_FILE', f'data/logs/{spider.name}/{spider.name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log')
    spider.settings.set('LOG_LEVEL', 'DEBUG')
    

class BaseSpider(scrapy.Spider):
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BaseSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider_opened, signal=signals.spider_opened)
        return spider

    def __init__(self, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)