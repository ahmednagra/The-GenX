import json
import re
from collections import OrderedDict

import scrapy
from datetime import datetime


class DieselSpiderSpider(scrapy.Spider):
    name = "diesel"
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
        }
    }

    def start_requests(self):
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        yield scrapy.Request(url='https://ae.diesel.com/', headers=headers, callback=self.parse)

    def parse(self, response, **kwargs):
        headers = response.request.headers
        headers['referer'] = response.url
        main_cats = ['SALE', "MAN", "WOMAN", "1DR", "KIDS"]
        for i in range(0, 5):
            main_cat_name = main_cats[i]
            cat = response.css('[data-action="navigation"] li')[i]
            urls = cat.css('.second-level .nav-anchor::attr(href)').getall()
            name = cat.css('.second-level .nav-anchor::attr(title)').getall()
            unique_names = list(set(name))
            unique_urls = list(set(urls))
            for url, sub_cat_name in zip(unique_urls, unique_names):
               yield scrapy.Request(url, callback=self.listing_page,
                                    headers=headers, meta={'main_cat_name': main_cat_name,
                                                           'sub_cat_name': sub_cat_name})

    def listing_page(self, response):
        main_cat_name = response.meta.get('main_cat_name')
        sub_cat_name = response.meta.get('sub_cat_name')
        headers = response.request.headers
        headers['referer'] = response.url

        products = response.css('.product-item-link::attr(href)').getall()
        for product in products:
            yield scrapy.Request(url=product, callback=self.product_page, headers=headers, meta={'main_cat_name': main_cat_name,
                                                                                                 'sub_cat_name': sub_cat_name})
        next_page_url = response.css('[title="Next"]::attr(href)').get('')
        if next_page_url:
            yield scrapy.Request(url=next_page_url, callback=self.listing_page, headers=headers,
                                 meta={'main_cat_name': main_cat_name,
                                       'sub_cat_name': sub_cat_name})

    def product_page(self, response):
        main_cat_name = response.meta.get('main_cat_name')
        sub_cat_name = response.meta.get('sub_cat_name')
        old_price = response.css('[data-price-type="oldPrice"] span::text').get('')

        try:
            json_data = json.loads(response.css('[type="application/ld+json"]::text').getall()[-1])
        except json.JSONDecodeError as e:
            json_data = {}
            print('json error :', e)
            return

        product_title = json_data.get('name', '')
        product_price = json_data.get('offers', {})[0].get('price', '')
        discount = 0.0
        if old_price:
            old_price = old_price.replace(',', '')
            match = re.search(r'\d+', old_price)
            if match:
                old_price = float(match.group())
                discount = old_price - float(product_price)
        currency = json_data.get('offers', {})[0].get('priceCurrency', '')
        availability = json_data.get('offers', {})[0].get('availability', '')
        brand = json_data.get('brand', {}).get('name', '')
        sku = json_data.get('sku', '')
        product_images = response.css('.image-gallery .image-gallery__item::attr(src)').getall()
        description = response.css('.description .value::text').get()
        colors = response.css('.micro_colour::text').get()
        if colors:
            colors = [colors]
        addtitional_desc = response.css('.additional-description span::text').getall()
        current_time = datetime.now()
        if addtitional_desc and description:
            addtitional_desc = '\n'.join(addtitional_desc)
            description = description + '\n' + addtitional_desc
        if addtitional_desc and description is None:
            description = addtitional_desc

        # Adjusted regex to capture the entire "jsonSwatchConfig" object
        pattern = r'"jsonSwatchConfig": (\{.*?\}\})'
        match = re.search(pattern, response.text, re.DOTALL)  # Enable multi-line matching
        sizes = variation = {}
        labels = length = []
        if match:
            json_string = match.group(1)  # Get the full JSON string

            try:
                size_data = json.loads(json_string)  # Parse into Python dict
                # print("Parsed JSON Object:")
                labels = [
                    value['label']
                    for key, value in size_data.get('192', {}).items()
                    if isinstance(value, dict) and 'label' in value
                ]
                length = [
                    value['label']
                    for key, value in size_data.get('200', {}).items()
                    if isinstance(value, dict) and 'label' in value
                ]
                if colors is None:
                    colors = [
                        value['label'].strip()
                        for key, value in size_data.get('209', {}).items()  # Use .get() to safely access '209'
                        if isinstance(value, dict) and 'label' in value
                    ]

                # Output the result
            except json.JSONDecodeError as e:
                a=1
                # print(f"Error decoding JSON: {e}")
        else:
            a=1
            # print("JSON object not found")

        sizes['us'] = labels

        if length:
            variation['length'] =  length
        # yield {
        #     "source_id": "Diesel",
        #     "product_url": response.url,
        #     "brand": brand,
        #     "product_title": product_title,
        #     "product_id": sku,
        #     "category": (main_cat_name + ', ' + sub_cat_name) if main_cat_name and sub_cat_name else main_cat_name,
        #     "price": float(product_price) if product_price else None,
        #     "discount": discount if discount else 0.0,
        #     "currency": currency,
        #     "description": description,
        #     "main_image_url": product_images[0] if product_images and len(product_images) >= 1 else '',
        #     "other_image_urls": product_images[1:] if product_images and len(product_images) > 1 else [],
        #     "colors": colors if colors else [],
        #     "other_details": self.other_detals(response),
        #     "availability": 'in_stock' if 'InStock' in availability else 'out_of_stock',
        #     "number_of_items_in_stock": 0,
        #     "Variations": variation if variation else {},
        #     "sizes": sizes if sizes else {},
        #     "last_update": "",
        #     "creation_date": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        #
        # }

        item = OrderedDict()
        item["source_id"] = "Diesel"
        item["product_url"] = response.url
        item["brand"] = brand
        item["product_title"] = product_title
        item["product_id"] = sku
        item["category"] = (main_cat_name + ', ' + sub_cat_name) if main_cat_name and sub_cat_name else main_cat_name
        item["price"] = float(product_price) if product_price else 0.0
        item["discount"] = discount if discount else 0.0
        item["currency"] = currency
        item["description"] = description
        item["main_image_url"] = product_images[0] if product_images and len(product_images) >= 1 else ''
        item["other_image_urls"] = product_images[1:] if product_images and len(product_images) > 1 else []
        item["colors"] = colors if colors else []
        item["variations"] = variation if variation else {}
        item["sizes"] = sizes if sizes else {}
        item["other_details"] = self.other_detals(response)
        item["availability"] = 'in_stock' if 'InStock' in availability else 'out_of_stock'
        item["number_of_items_in_stock"] = 0
        item["last_update"] = ""
        item["creation_date"] = current_time.strftime("%Y-%m-%d %H:%M:%S")

        yield item

    @staticmethod
    def other_detals(response):
        care_instructions = response.css('.care-instructions-content li::text').getall()
        size_and_fit = response.css('.size-fit-content li span::text').get('')

        other_details = {
            "care_instructions": [line.strip() for line in care_instructions if line.strip()] if care_instructions else [],
            "size_and_fit": size_and_fit.strip() if size_and_fit else ''
        }
        return other_details if other_details else {}
