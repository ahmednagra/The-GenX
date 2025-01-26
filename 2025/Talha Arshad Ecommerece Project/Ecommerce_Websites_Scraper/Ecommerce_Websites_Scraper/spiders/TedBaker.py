import os
import re
import json
from math import ceil
from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict

import time

import requests
from scrapy import Spider, Request


class TedbakerSpider(Spider):
    name = "TedBaker"
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        "CONCURRENT_REQUESTS": 3,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'DOWNLOAD_TIMEOUT': 70,

        'FEEDS': {
            f'output/{name} Products Details {current_dt}.json': {
                'format': 'json',
                'encoding': 'utf8',
                'indent': 4,
                'fields': [
                    'source_id', 'product_url', 'brand', 'product_title', 'product_id', 'category', 'price', 'discount',
                    'currency', 'description', 'main_image_url', 'other_image_urls', 'colors', 'variations',
                    'sizes', 'other_details', 'availability', 'number_of_items_in_stock', 'last_update', 'creation_date'
                ]
            }
        },
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'priority': 'u=0, i',
        'referer': 'https://tedbaker.sa',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    json_headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'referer': 'https://tedbaker.sa/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'script',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

    def __init__(self):
        super().__init__()
        self.item_found = 0
        self.item_scraped = 0
        self.scraped_records = []
        self.urls = []

        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def start_requests(self):
        urls = [
                # 'https://tedbaker.sa/collections/women',
                # 'https://tedbaker.sa/collections/men',
                'https://tedbaker.sa/collections/gifts'
                ]
        for url in urls:
            cat= (url.split('/')[-1]).title()
            print('Category Called', cat)
            yield Request(url, callback=self.parse, headers=self.headers, dont_filter=True, meta={'category':cat})

    def parse(self, response, **kwargs):
        category = response.meta.get('category', '')
        yield from self.parse_pagination(response)
        try:
            total_products = response.css('section .js-boost-items-count ::text').re_first(r'\d[\d,]*')

            if not response.meta.get('page_no', ''):
                self.write_logs(f"Category: {category.title()} | Total Items Found: {total_products}")
                self.item_found += int(total_products)
                total_pages = ceil(int(total_products)/24)
                for page_no in range(2, total_pages + 1):
                    collection_scope = response.css('#__st ::text').get('').split('"collection","rid":')[1].replace('};', '')
                    sid = ''
                    data = self.form_data(page_no, collection_scope, sid)
                    # page= f'?page={page_no}'
                    # next_page = urljoin(response.url, page)
                    # response.meta['page_no'] = pg_no
                    # a = f'https://tedbaker.sa/collections/gifts?page=3'
                    # requests.get(, params=params, headers=headers)
                    # url = 'https://services.mybcapps.com/bc-sf-filter/filter'
                    # yield Request(url, callback=self.parse_pagination, headers=self.json_headers, body=json.dumps(data), dont_filter=True, meta=response.meta)
                    req = requests.get('https://services.mybcapps.com/bc-sf-filter/filter', params=data, headers=self.json_headers)
                    response_text = req.text
                    json_start = response_text.find('(') + 1
                    json_end = response_text.rfind(')')
                    json_data = response_text[json_start:json_end]
                    data = json.loads(json_data)
        except Exception as e:
            print(f'Error in Parse Function:{e} URL:{response.url}')

    def parse_pagination(self, response):
        try:
            products_tag = response.css('#web-pixels-manager-setup ::text').get('').split('collection_viewed", ')[1].replace('\\', '')
            pro = json.loads(products_tag.split(');},')[0])
        except json.JSONDecodeError as e:
            pro= {}
            print(f'Error in Product Dictionary:{e}  URL{response.url}')

        products = pro.get('collection', {}).get('productVariants', [])
        for product in products:
            url = product.get('product', {}).get('url', '')
            p_url = f'https://tedbaker.sa{url}'
            # url = urljoin(response.url, url)
            self.urls.append(url) #test
            response.meta['product']= product
            yield Request(p_url, callback=self.parse_product_details, headers=self.headers, meta=response.meta)

    def parse_product_details(self, response):
        product = response.meta.get('product', {})
        # price= product.get('price', {}).get('amount', 0.0)
        cat= response.meta.get('category', '')
        cat = cat.title() if cat else ''

        try:
            data_dict = response.css('script[type="application/ld+json"]:contains(": \\"Product")::text').get('')
            data_dict = data_dict.replace('\t', '').replace('" / ', '')
            data_dict = json.loads(data_dict)
        except:
            data_dict = {}
            return

        item = OrderedDict()
        item['source_id'] = 'TedBaker'
        item['brand'] = product.get('product', {}).get('vendor', '')
        item['product_title'] = product.get('product', {}).get('title', '')
        item['category'] = cat
        item['description'] = data_dict.get('description', '')
        item['main_image_url'] = response.css('meta[property="og:image:secure_url"] ::attr(content)').get('')
        item['other_image_urls'] = self.get_image(response)
        item['number_of_items_in_stock'] = 0
        item['last_update'] = ''
        item['creation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        variations = data_dict.get('offers', [])
        for variation in variations:
            try:
                availability = variation.get('availability', '')
                previous_price = response.css('.o-product-form-1__price--old ::text').re_first(r'\d[\d,]*')
                previous_price = previous_price.replace(',', '') if previous_price else '0'
                current_price = variation.get('price', '')
                discount = int(previous_price) - float(current_price) if int(previous_price) > float(
                    current_price) else 0

                item['product_id'] = variation.get('sku', '')
                item['price'] = float(current_price) if current_price else 0.0
                item['discount'] = float(discount) if discount else 0.0
                item['currency'] = variation.get('priceCurrency', '')
                item['colors'] = [variation.get('color', '')] or []
                item['variations'] = {}
                item['sizes'] = self.get_sizes(variation)
                item['other_details'] = {}
                item['availability'] = 'in_stock' if 'InStock' in availability else 'out_of_stock'
                item['product_url'] = variation.get('url', '')
                self.item_scraped += 1
                yield item
            except Exception as e:
                print(f'Error parsing item:{e} URL:{response.url}')
                self.item_scraped += 1
                yield item

    def price_format(self, price):
        try:
            price = str(int(price))
        except:
            price = str(price)

        return price

    def get_image(self, response):
        unique_images = set()

        # Extract all images from meta tags with 'og:image'
        og_images = response.css('meta[property="og:image"]::attr(content)').getall()
        if og_images:
            v_id = next((''.join(img.split('v=')[1:2]) for img in og_images if 'v=' in img), None)
            unique_images.update(og_images)

        # Extract lazy-loaded images
        lazy_images = response.css('img.lazyload::attr(data-src)').getall()
        if lazy_images:
            filtered_lazy_images = [img for img in lazy_images if v_id and v_id in img]
            base_url = "https:"
            for img in filtered_lazy_images:
                if img.startswith("//"):
                    full_image_url = f"{base_url}{img}"
                else:
                    full_image_url = img
                unique_images.add(full_image_url)

        return list(unique_images)

    def get_sizes(self,variation):
        value = variation.get('size', '').lower()
        keys = ['uk', 'eu', 'us']
        if value:
            info = {key: value for key in keys if key in value} or {'us': value}
            return info
        else:
            return {}

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def form_data(self, page_no, collection_scope, sid):
        # Get the current time in seconds since the Unix epoch
        timestamp_seconds = int(time.time())
        timestamp_milliseconds = timestamp_seconds * 1000
        params = {
            # 't': '1737880096779',
            't': timestamp_milliseconds,
            # 'page': '12',
            'page': page_no,
            '_': 'pf',
            'shop': 'ted-baker-ksa.myshopify.com',
            'limit': '24',
            'sort': 'created-descending',
            'display': 'grid',
            # 'collection_scope': '446925308212',
            'collection_scope': collection_scope,
            'tag': '',
            'product_available': 'true',
            'variant_available': 'true',
            'build_filter_tree': 'true',
            'check_cache': 'false',
            'sort_first': 'available',
            'locale': 'en',
            # 'sid': '47704006-20e8-40b0-8367-6803b9b01990',
            # 'sid': sid,
            'callback': 'BoostPFSFilterCallback',
            'event_type': 'init',
        }
        return params

    def close(spider, reason):
        # Log overall scraping statistics
        spider.write_logs(f"\n--- Scraping Summary ---")
        spider.write_logs(f"Total Products Available on Website: {spider.item_found}")
        spider.write_logs(f"Total Products Successfully Scraped: {spider.item_scraped}")

        # Log script execution times
        spider.write_logs(f"\n--- Script Execution Times ---")
        spider.write_logs(f"Script Start Time: {spider.script_starting_datetime}")
        spider.write_logs(f"Script End Time: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        spider.write_logs(f"Reason for Closure: {reason}")
