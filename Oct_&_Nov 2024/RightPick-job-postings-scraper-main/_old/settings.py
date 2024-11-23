BOT_NAME = 'scraper_scrapy'

SPIDER_MODULES = ['scraper_scrapy']
NEWSPIDER_MODULE = 'scraper_scrapy'

# Enable and configure the Splash middleware
SPLASH_URL = 'http://0.0.0.0:8050'

DOWNLOADER_MIDDLEWARES = {
    'scrapy_splash.SplashCookiesMiddleware': 723,
    'scrapy_splash.SplashMiddleware': 725,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
}

SPIDER_MIDDLEWARES = {
    'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
}

# Enable your custom pipeline
ITEM_PIPELINES = {
    'scraper.MckinseyJobsPipeline': 300,
}