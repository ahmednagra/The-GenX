from shutil import which

from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())
SCRAPEOPS_API_KEY = os.getenv('SCRAPEOPS_API_KEY')

# SEEN_JOBS_FILE = 'info/seen_jobs.json'

# Scrapy settings for jobscraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html


BOT_NAME = "jobscraper"

SPIDER_MODULES = ["jobscraper.spiders"]
NEWSPIDER_MODULE = "jobscraper.spiders"

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "jobscraper (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 1

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "jobscraper.middlewares.JobscraperSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html

SCRAPEOPS_PROXY_ENABLED = True

DOWNLOADER_MIDDLEWARES = {
    #    "jobscraper.middlewares.JobscraperDownloaderMiddleware": 543,
    # Log the User-Agent for each request made
    'jobscraper.middlewares.UserAgentLoggerMiddleware': 1000,
    
    # # Rotating User Agents
    # 'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    # 'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 800,
    # 'jobscraper.middlewares.ScrapeOpsFakeUserAgentMiddleware': 800,

    # # Rotating Free Proxies
    # 'scrapy_proxy_pool.middlewares.ProxyPoolMiddleware': 610,
    # 'scrapy_proxy_pool.middlewares.BanDetectionMiddleware': 620,

    ## ScrapeOps Proxy SDK
    'scrapeops_scrapy.middleware.retry.RetryMiddleware': 550,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    'scrapeops_scrapy_proxy_sdk.scrapeops_scrapy_proxy_sdk.ScrapeOpsScrapyProxySdk': 725,
    # 'jobscraper.middlewares.ScrapeOpsProxyMiddleware': 725,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    'scrapeops_scrapy.extension.ScrapeOpsMonitor': 500, 
    "scrapy.extensions.telnet.TelnetConsole": None,
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'jobscraper.pipelines.JobPipeline': 50,
    'jobscraper.pipelines.DuplicatesPipeline': 100,
    'jobscraper.pipelines.FirestoreUpdatePipeline': 200,
    'jobscraper.pipelines.RemoveOutdatedJobsPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
FEED_EXPORT_ENCODING = "utf-8"

# FEEDS = {
#     'data/global_jobs.csv': {
#         'format': 'csv',
#         'item_export_kwargs': {
#             'export_empty_fields': True,
#             'include_headers_line': not os.path.exists('data/global_jobs.csv')
#         },
#         'overwrite': False
#     },
#     'data/%(name)s_jobs.csv': {
#         'format': 'csv',
#         'item_export_kwargs': {
#             'export_empty_fields': True,
#             'include_headers_line': False
#         },
#         'overwrite': False
#     },
#     'data/logs/%(name)s/%(name)s_jobs_%(time)s.csv': {
#         'format': 'csv',
#         'item_export_kwargs': {
#             'export_empty_fields': True,
#         },
#     }
# }

FEED_EXPORTERS = {
    'json': 'scrapy.exporters.JsonItemExporter',
}

FEEDS = {
    'data/global_jobs.json': {
        'format': 'json',
        'item_export_kwargs': {
            'export_empty_fields': True,
        },
        'overwrite': False
    },
    'data/%(name)s_jobs.json': {
        'format': 'json',
        'item_export_kwargs': {
            'export_empty_fields': True,
        },
        'overwrite': False
    },
    'data/logs/%(name)s/%(name)s_jobs_%(time)s.json': {
        'format': 'json',
        'item_export_kwargs': {
            'export_empty_fields': True,
        },
    }
}

DOWNLOAD_HANDLERS = {
    # "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    # "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
    "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# # To Store in AWS S3 Bucket
# AWS_ACCESS_KEY_ID = 'myaccesskeyhere'
# AWS_SECRET_ACCESS_KEY = 'mysecretkeyhere'

# PLAYWRIGHT_LAUNCH_OPTIONS = {
#     'headless': True
# }