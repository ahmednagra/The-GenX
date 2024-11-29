import os
import re
from datetime import datetime
from collections import OrderedDict
from urllib.parse import urljoin, unquote, urlparse, parse_qs

from scrapy import signals, Spider, Request, Selector


class SpokenGospelSpider(Spider):
    name = "spokengospel"
    base_url = 'https://www.spokengospel.com'
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        "CONCURRENT_REQUESTS": 2,
        "RETRY_TIMES": 7,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 400, 403, 404, 408],
        "FEEDS": {
            f"output/Spoken Gospel Articles Detail {current_dt}.csv": {
                "format": "csv",
                "fields": ['filename', 'item_title', 'item_subtitle', 'item_image', 'item_url',
                           'url_main', 'item_summary', 'type', 'hierarchy1', 'content']
            }
        },
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    }

    def __init__(self):
        super().__init__()
        self.items_scraped = 0
        self.categories = [{'Old Testament': 'data-w-tab="ot"'}, {'New Testament': 'data-w-tab="nt"'}]
        # self.categories = [{'New Testament': 'data-w-tab="nt"'}]

        # Logs
        os.makedirs("logs", exist_ok=True)
        self.logs_filepath = f"logs/Crosswalk Logs {self.current_dt}.txt"
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f"[INIT] Script started at {self.script_starting_datetime}")

    def parse_category(self, response, **kwargs):
        cat_id = response.meta.get('id', '')
        category = response.meta.get('category', '')

        if 'ot' in cat_id:
            cat_items_urls = response.css('div[data-w-tab="ot"] [role="listitem"] a::attr(href)').getall()
        else:
            cat_items_urls = response.css('div[data-w-tab="nt"] [role="listitem"] a::attr(href)').getall()

        self.write_logs(f'Category: {len(cat_items_urls)} Sections Found')

        for cat_item_url in cat_items_urls:
            url = urljoin(self.base_url, cat_item_url)
            print(f' Sub Cat Url : {url}')
            yield Request(url, headers=self.headers, callback=self.parse_category_item, meta={'category': category})

    def parse_category_item(self, response):
        category = response.meta.get('category', '')

        try:
            item_url = self.get_youtube_url(response)
            item_title = response.css('#introduction .hero-item h2::text').get('').strip().title()
            item = OrderedDict()
            item['filename'] = f'spokengospel_{item_title}_video'
            item['item_title'] = item_title
            item['item_subtitle'] = response.css('#introduction .hero-item p::text').get('').strip().title()
            item['item_image'] = response.css('.book-photo img[loading="eager"]::attr(src)').get('')
            item['item_url'] = item_url
            item['url_main'] = response.url
            item['item_summary'] = ''
            item['type'] = 'video' if item_url else ''
            item['hierarchy1'] = ''
            item['content'] = ''

            self.items_scraped += 1
            print('Items Scraped', self.items_scraped)
            yield item

        except Exception as e:
            self.write_logs(f'Error Yield item {response.url}   Error:{e}')

        hierarchy1 = response.css('.book-title ::text').get('').strip().title()
        section = response.css('html::attr(data-wf-item-slug)').get('')
        meta_item = {
            'hierarchy1': hierarchy1,
            'category': category,
            'section': section
        }

        devotionals_urls = list(set(response.css('#devos-expand .w-dyn-item a::attr(href)').getall()))
        for dev_url in devotionals_urls:
            url = urljoin(self.base_url, dev_url)
            print(f' Devotional Url : {url}')
            yield Request(url, headers=self.headers, callback=self.parse_devotional_detail, meta=meta_item)

        next_page = response.css('.w-pagination-next ::attr(href)').get('')
        if next_page:
            url = urljoin(response.url, next_page)
            print(f' Next Page Url : {url}')
            yield Request(url, headers=self.headers, callback=self.pagination, meta=meta_item)

    def pagination(self, response):
        devotionals_urls = list(set(response.css('#devos-expand .w-dyn-item a::attr(href)').getall()))
        for dev_url in devotionals_urls:
            url = urljoin(self.base_url, dev_url)
            print(f' Devotional Url : {url}')
            yield Request(url, headers=self.headers, callback=self.parse_devotional_detail, meta=response.meta)

    def parse_devotional_detail(self, response):
        a=1
        category = response.meta.get('category', '')
        hierarchy1 = response.meta.get('hierarchy1', '')
        section = response.meta.get('section', '')
        try:
            item_title = response.css('.book-title::text').get('')
            common_item = OrderedDict()
            common_item['item_title'] = item_title
            common_item['item_subtitle'] = response.css('.book-intro-title::text').get('')
            common_item['item_image'] = response.css('.collage_wrapper-inner img::attr(src)').getall()[-1]
            common_item['hierarchy1'] = hierarchy1
            common_item['item_summary'] = ''.join(response.css('.hero-item p ::text').getall())

            read_item = OrderedDict()
            read_item.update(common_item)  # Add common_item details to read_item
            read_item['filename'] = f'spokengospel_{item_title}_text'
            read_item['item_url'] = f'{response.url}#read'
            read_item['url_main'] = ''
            read_item['type'] = 'Website'
            read_item['content'] = self.get_content(response, filename=read_item['filename'])

            self.items_scraped += 1
            print('Items Scraped', self.items_scraped)
            yield read_item

            video_item = OrderedDict()
            video_item.update(common_item)  # Add common_item details to video_item
            video_item['filename'] = f'spokengospel_{item_title}_video'
            video_item['item_url'] = self.get_youtube_url(response)
            video_item['url_main'] = f'{response.url}#read'
            video_item['type'] = 'video'
            video_item['content'] = ''

            self.items_scraped += 1
            print('Items Scraped', self.items_scraped)
            yield video_item

        except Exception as e:
            self.write_logs(f'Error yield item :{category}> {section} URL:{response.url}')
            a=1

    def get_youtube_url(self, response):
        iframe_src = response.css('.embedly-embed::attr(src)').get('')
        decoded_iframe_src = unquote(iframe_src)

        try:
            parsed_url = urlparse(decoded_iframe_src)
            query_params = parse_qs(parsed_url.query)
            url = query_params.get("url", [""])[0]
        except:
            video_id = decoded_iframe_src.split('?v=')[1].split('&image')
            url = f'https://www.youtube.com/watch?v={video_id}'

        return url if url else ''

    def get_content(self, response, filename):
        text = '\n\n'.join(response.css('.devo-content.w-richtext ::text').getall())

        output_folder = "output"
        try:
            # Check if the output folder exists, create it if not
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
                self.write_logs(f"Created output folder: {output_folder}")

            # Clean and validate the filename
            cleaned_filename = re.sub(r'[<>:"/\\|?*]', "", filename)
            if not cleaned_filename:
                self.write_logs(f"Invalid filename after cleaning: {filename}")
                return text

            # Write the text to the specified file
            file_path = os.path.join(output_folder, f"{cleaned_filename}.txt")
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(text)

            self.write_logs(f"Successfully wrote to file: {file_path}")
        except Exception as e:
            self.write_logs(
                f"ile Name: {filename}  An error occurred while getting information: {e}"
            )

        return text

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode="a", encoding="utf-8") as logs_file:
            logs_file.write(f"{log_msg}\n")
            print(log_msg)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SpokenGospelSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        if self.categories:
            categories = self.categories.pop(0)
            key, value = next(iter(categories.items()))
            self.write_logs(
                f"Section :{key.title()} Is called for Category Scraping"
            )
            req = Request(
                url='https://www.spokengospel.com/library',
                callback=self.parse_category,
                dont_filter=True,
                headers=self.headers,
                meta={"handle_httpstatus_all": True, "category": key, 'id':value},
            )

            try:
                self.crawler.engine.crawl(req)  # For latest Python version
            except TypeError:
                self.crawler.engine.crawl(req, self)  # For old Python version < 10

    def close(spider, reason):
        spider.write_logs(f"Total Articles are Scraped: {spider.items_scraped}")
        spider.write_logs(f"Script Started at: {spider.script_starting_datetime}")
        spider.write_logs(
            f'Script Stopped at: {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}'
        )
