import os
import re
import json
from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict

from scrapy import signals, Spider, Request, FormRequest


class FarfetchSpider(Spider):
    name = "FarFetch"
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        # "CONCURRENT_REQUESTS": 3,
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

        "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        'DOWNLOADER_MIDDLEWARES': {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
            "scrapy_poet.InjectionMiddleware": 543,
        },
        'REQUEST_FINGERPRINTER_CLASS': "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        'ZYTE_API_KEY': "f693db95c418475380b0e70954ed0911",
        "ZYTE_API_TRANSPARENT_MODE": True,
    }

    def __init__(self):
        super().__init__()
        self.current_records = []
        self.current_category = ''
        self.category_item_found = 0
        self.category_item_scraped = 0
        self.categories_item_found = 0
        self.categories_item_scraped = 0
        self.categories = ['women', 'men', 'kids']
        self.count_categories = len(self.categories)

        #files & Records
        os.makedirs('output', exist_ok=True)
        self.output_file_path = f'output/{self.name} Products Details {self.current_dt}.json'

        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def parse_category_brands(self, response, **kwargs):
        if response.status !=200:
            return

        try:
            script_tag = response.css('script:contains("HYDRATION_STATE") ::text').re_first(
                r'designersByLetter(.*?)"context')
            script_tag = script_tag.replace('\\', '').replace('":{', '{').replace(']},', ']}')
            brand_dit = json.loads(script_tag)
        except json.JSONDecodeError as e:
            print(f'Json Error: {e} URL:{response.url}')
            return

        for alphabet_brands, brands in brand_dit.items():
            for brand in brands:
                brand_url = brand.get('href', '')
                yield Request(url=f'https://www.farfetch.com{brand_url}',
                              callback=self.parse_brand, dont_filter=True,
                              meta={"handle_httpstatus_all": True})

    def parse_brand(self, response):
        try:
            product_dict = json.loads(response.css('script:contains("ItemList") ::text').get(''))
        except json.JSONDecodeError as e:
            self.write_logs(f'Sorry, no items were found: URL{response.url}')
            product_dict = {}
            return

        products = product_dict.get('itemListElement', [])

        if products:
            self.category_item_found += len(products)
            self.categories_item_found += len(products)

        for product in products:
            prod_url = product.get('offers', {}).get('url', '')
            url = f'https://www.farfetch.com{prod_url}'

            #Avoid Duplications
            if url in self.current_records:
                print(f"{product.get('name', '').strip().title()} already scraped")
                continue

            yield Request(url, callback=self.parse_product_detail,
                          meta={"handle_httpstatus_all": True, 'product':product})

    def parse_product_detail(self, response):
        try:
            info_dict = json.loads(response.css('script[type="application/ld+json"]:contains("Product") ::text').get(''))
            cat_dict = json.loads(response.css('script:contains("BreadcrumbList") ::text').get(''))
            cat_dict = cat_dict.get('itemListElement', [])
        except json.JSONDecodeError as e:
            print(f'Product Information error: {e}')
            info_dict = {}
            cat_dict = {}
            return

        try:
            title = info_dict.get('name', '')
            colors = info_dict.get('color', '')
            p_id = info_dict.get('productID', '')
            brand = info_dict.get('brand', {}).get('name', '')
            in_stock = info_dict.get('offers', {}).get('availability', '')
            original_price = response.css('[data-component="PriceOriginal"] ::text').re_first(r'\d[\d,]*')
            image_urls = [img.get('contentUrl', '') for img in info_dict.get('image', []) if img.get('contentUrl', '')]

            original_price = float(original_price.replace(',', '')) if original_price else 0.0
            current_price = float(info_dict.get('offers', {}).get('price', 0) or 0.0)

            no_items = response.css('.ltr-knpsgl p::text').re_first(r'\d[\d,]*') or response.css('.ltr-xct4wp p:contains("left") ::text').re_first(r'\d[\d,]*')
            no_items = int(no_items.replace(',', '')) if no_items else 0

            item = OrderedDict()
            item['source_id'] = 'FarFetch'
            item['product_url'] = response.url
            item['brand'] = str(brand).strip().title() if isinstance(brand, str) else ''
            item['product_title'] =  str(title).strip().title() if isinstance(title, str) else ''
            item['product_id'] = p_id
            item['category'] = ', '.join([cat.get('item', {}).get('name', '') for cat in cat_dict])
            item['price'] = float(current_price)
            item['discount'] = original_price - current_price if original_price > current_price else 0.0
            item['currency'] = info_dict.get('offers', {}).get('priceCurrency', '')
            item['description'] = self.get_description(response, tag='.exjav154 > div')
            item['main_image_url'] = ''.join([img.get('contentUrl', '') for img in info_dict.get('image', [])][0:1])
            item['other_image_urls'] = image_urls[1:5] if image_urls[1:5] else []
            item['colors'] = [colors] if colors else []
            item['other_details'] = self.get_other_details(response)
            item['availability'] = 'in_stock' if in_stock and in_stock is not None else 'out_of_stock'
            item['number_of_items_in_stock'] = no_items if no_items and float(no_items)!=current_price else None
            item['last_update'] = ''
            item['creation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if p_id:
                data = self.get_form_data(p_id)
                headers = {
                    'Content-Type': 'application/json',  # Set correct Content-Type for JSON body
                    'Accept': 'application/json',  # Ensure JSON response is expected
                    'User-Agent': 'Mozilla/5.0 (compatible; Scrapy)',  # Identify your spider
                }
                url = 'https://www.farfetch.com/sa/experience-gateway'
                response.meta['item']= item
                response.meta['handle_httpstatus_all'] = True
                yield FormRequest(url=url, callback=self.product_size, method='POST', formdata=None,
                                                body=json.dumps(data), headers=headers, meta=response.meta)

            else:
                self.category_item_scraped += 1
                self.categories_item_scraped += 1
                yield item
        except Exception as e:
            self.write_logs(f'Error parsing item:{e} URL:{response.url}')
    def product_size(self, response):
        item = response.meta.get('item', OrderedDict())

        try:
            size = {}
            variation = {}
            data_dict = response.json().get('data', {}).get('product', {})
            v_key = data_dict.get('scale', {}).get('description', '')  # Key for the variations
            v_key = re.sub(r'[^a-zA-Z0-9]+', '_', v_key.strip().lower()).strip('_')
            # print('Variation key:', v_key)
            # v_id = data_dict.get('scale', {}).get('id', '')
            edges = data_dict.get('variations', {}).get('edges', [])

            if v_key:
                size[v_key] = []
                # variation[v_key] = []

            for edge in edges:
                node = edge.get('node', {})
                try:
                    price = float(node.get('price', {}).get('value', {}).get('raw', 0.0))
                    value = node.get('variationProperties', [])[0].get('values', [])[0].get('description', '')

                    if v_key and value:
                        size[v_key].append({
                            "size_value": value,
                            "price": price
                        })
                except Exception as e:
                    # print(f"Error processing size data: {e}")
                    a = 1

                # Variation processing
                try:
                    measurements = node.get('measurements', [])
                    if measurements:
                        variation = {
                            m.get('name', '').lower(): [
                                m.get('imperial', {}).get('formatted', ''),
                                m.get('metric', {}).get('formatted', '')
                            ]
                            for m in measurements
                        }

                        # Check if variation is empty
                        if not variation:
                            # print("No variation data found.")
                            a=1
                    else:
                        a=1
                        # print("No measurements available.")
                except Exception as e:
                    # print(f"Error processing variation data: {e}")
                    a = 1

            item['variations'] = variation if variation else {}
            item['sizes'] = size if size else {}

        except Exception as e:
            a=1

        self.category_item_scraped += 1
        self.categories_item_scraped += 1
        self.current_records.append(item['product_title'])
        yield item

    def get_description(self,response, tag):
        text = []
        # tags = response.css('.exjav154 > div')
        tags = response.css(f'{tag}')
        # If tags list is empty, return an empty string
        if not tags:
            return ''

        for tag in tags[:1]:
            if 'data-component="Img"' in tag.get():
                continue
            tag_texts = tag.xpath('.//text()[not(ancestor::style)]').getall()
            tag_texts = '\n'.join(tag_texts)
            text.append(''.join(tag_texts))

        return '\n'.join(text)

    def get_other_details(self, response):
        details = {}
        try:
            div_tags = response.css('.exjav152 .ltr-92qs1a')
            for tag in div_tags:
                section_title = tag.css('h4::text').get('')
                details_list = tag.css('p').xpath('string(.)').getall()  # Get text content of <p> and nested <span>

                # Clean and format the details
                details_list = [detail.strip() for detail in details_list if
                                detail.strip()]  # Remove empty strings and strip whitespace

                # Add to the dictionary
                if section_title:
                    details[section_title] = details_list
        except Exception as e:
            a=1

        return details

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def get_form_data(self,p_id):
        json_data = {
            'operationName': 'SizeAndFitData',
            'variables': {
                # 'productId': '13503357',
                'productId': str(p_id),
            },
            'query': 'query SizeAndFitData($productId: ID!, $merchantId: ID) {\n  user {\n    id\n    preference {\n      unitSystem\n      __typename\n    }\n    __typename\n  }\n  product(id: $productId, merchantId: $merchantId) {\n    ... on Product {\n      id\n      scale {\n        id\n        description\n        abbreviation\n        isOneSize\n        __typename\n      }\n      variations {\n        edges {\n          node {\n            ... on Variation {\n              id\n              price {\n                value {\n                  raw\n                  formatted\n                  __typename\n                }\n                __typename\n              }\n              images {\n                order\n                size1000 {\n                  url\n                  alt\n                  __typename\n                }\n                modelVariationSize\n                modelMeasurement {\n                  height {\n                    type\n                    name\n                    imperial {\n                      raw\n                      formatted\n                      __typename\n                    }\n                    metric {\n                      raw\n                      formatted\n                      __typename\n                    }\n                    __typename\n                  }\n                  bodyMeasurements {\n                    type\n                    name\n                    imperial {\n                      raw\n                      formatted\n                      __typename\n                    }\n                    metric {\n                      raw\n                      formatted\n                      __typename\n                    }\n                    __typename\n                  }\n                  __typename\n                }\n                __typename\n              }\n              fitting {\n                type\n                description\n                __typename\n              }\n              measurements {\n                type\n                name\n                imperial {\n                  raw\n                  formatted\n                  __typename\n                }\n                metric {\n                  raw\n                  formatted\n                  __typename\n                }\n                __typename\n              }\n              variationProperties {\n                ... on ScaledSizeVariationProperty {\n                  order\n                  values {\n                    id\n                    order\n                    description\n                    scale {\n                      id\n                      description\n                      __typename\n                    }\n                    __typename\n                  }\n                  __typename\n                }\n                __typename\n              }\n              __typename\n            }\n            ... on VariationUnavailable {\n              product {\n                id\n                __typename\n              }\n              images {\n                order\n                size1000 {\n                  url\n                  alt\n                  __typename\n                }\n                modelVariationSize\n                modelMeasurement {\n                  height {\n                    type\n                    name\n                    imperial {\n                      raw\n                      formatted\n                      __typename\n                    }\n                    metric {\n                      raw\n                      formatted\n                      __typename\n                    }\n                    __typename\n                  }\n                  bodyMeasurements {\n                    type\n                    name\n                    imperial {\n                      raw\n                      formatted\n                      __typename\n                    }\n                    metric {\n                      raw\n                      formatted\n                      __typename\n                    }\n                    __typename\n                  }\n                  __typename\n                }\n                __typename\n              }\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n',
        }

        return json_data

    def close(spider, reason):
        # Log overall scraping statistics
        spider.write_logs(f"\n--- Scraping Summary ---")
        spider.write_logs(f"Total Products Available on Website: {spider.categories_item_found}")
        spider.write_logs(f"Total Products Successfully Scraped: {spider.categories_item_scraped}")

        # Log script execution times
        spider.write_logs(f"\n--- Script Execution Times ---")
        spider.write_logs(f"Script Start Time: {spider.script_starting_datetime}")
        spider.write_logs(f"Script End Time: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        spider.write_logs(f"Reason for Closure: {reason}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(FarfetchSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        if  self.current_category:
            self.write_logs(f"Total Products Available on Category: {self.current_category} Are:{self.category_item_found}")
            self.write_logs(f"Total Products Scraped on Category: {self.current_category} Are:{self.category_item_scraped}")
            self.category_item_found = 0
            self.category_item_scraped = 0

        if self.categories:
            self.write_logs(f"\n\n{len(self.categories)}/{self.count_categories} Categories left to Scrape\n")

            self.current_category = self.categories.pop()
            self.write_logs(f"{ self.current_category.title()} Category is Starting to Scrape\n\n")

            url = f'https://www.farfetch.com/sa/designers/{ self.current_category}'
            self.crawler.engine.crawl(
                Request(url, callback=self.parse_category_brands, dont_filter=True,
                        meta={"handle_httpstatus_all": True}),
                spider=self
            )