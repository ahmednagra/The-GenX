import json
import os
import re
from datetime import datetime
from collections import OrderedDict
from math import ceil
from typing import Iterable
from urllib.parse import urljoin, unquote, urlparse, parse_qs

from scrapy import signals, Spider, Request, Selector


class WholefoodsmarketSpider(Spider):
    name = "wholefoodsmarket"
    base_url = 'https://www.spokengospel.com'
    start_urls = ['https://www.wholefoodsmarket.com/products/all-products?sort=brandaz']
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        "CONCURRENT_REQUESTS": 2,
        "RETRY_TIMES": 7,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 400, 403, 404, 408],
        "FEEDS": {
            f"wholefoodsmarket_output/Whole Foods Market Brands Details {current_dt}.csv": {
                "format": "csv",
                "fields": ['Category', 'Sub Category', 'Name', 'Brand', 'Brand Domain', 'Image_Url', 'Url']
            }
        },
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'content-type': 'application/json',
        'origin': 'https://brandfetch.com',
        'priority': 'u=1, i',
        'referer': 'https://brandfetch.com/',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    }

    def __init__(self):
        super().__init__()
        self.items_scraped = 0
        self.categories = [{'Old Testament': 'data-w-tab="ot"'}, {'New Testament': 'data-w-tab="nt"'}]
        # self.categories = [{'New Testament': 'data-w-tab="nt"'}]

        # Logs
        os.makedirs("logs", exist_ok=True)
        self.logs_filepath = f"logs/wholefoodsmarket Logs {self.current_dt}.txt"
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        # self.write_logs(f"[INIT] Script started at {self.script_starting_datetime}")

        self.results = [{'category': 'Produce', 'count': 181, 'sub_cat_slug': 'fresh-fruit', 'sub_category': 'Fresh Fruit'}, {'category': 'Produce', 'count': 43, 'sub_cat_slug': 'fresh-herbs', 'sub_category': 'Fresh Herbs'}, {'category': 'Produce', 'count': 288, 'sub_cat_slug': 'fresh-vegetables', 'sub_category': 'Fresh Vegetables'}, {'category': 'Dairy & Eggs', 'count': 42, 'sub_cat_slug': 'butter-margarine', 'sub_category': 'Butter & Margarine'}, {'category': 'Dairy & Eggs', 'count': 425, 'sub_cat_slug': 'cheese', 'sub_category': 'Cheese'}, {'category': 'Dairy & Eggs', 'count': 34, 'sub_cat_slug': 'eggs', 'sub_category': 'Eggs'}, {'category': 'Dairy & Eggs', 'count': 306, 'sub_cat_slug': 'yogurt', 'sub_category': 'Yogurt'}, {'category': 'Dairy & Eggs', 'count': 201, 'sub_cat_slug': 'dairy-alternatives', 'sub_category': 'Dairy Alternatives'}, {'category': 'Dairy & Eggs', 'count': 175, 'sub_cat_slug': 'milk-cream', 'sub_category': 'Milk & Cream'}, {'category': 'Meat', 'count': 88, 'sub_cat_slug': 'beef', 'sub_category': 'Beef'}, {'category': 'Meat', 'count': 90, 'sub_cat_slug': 'chicken', 'sub_category': 'Chicken'}, {'category': 'Meat', 'count': 25, 'sub_cat_slug': 'pork', 'sub_category': 'Pork'}, {'category': 'Meat', 'count': 17, 'sub_cat_slug': 'turkey', 'sub_category': 'Turkey'}, {'category': 'Meat', 'count': 36, 'sub_cat_slug': 'bacon', 'sub_category': 'Bacon'}, {'category': 'Meat', 'count': 70, 'sub_cat_slug': 'hotdogs-sausage', 'sub_category': 'Hotdogs & Sausage'}, {'category': 'Meat', 'count': 7, 'sub_cat_slug': 'game-meats', 'sub_category': 'Game Meats'}, {'category': 'Meat', 'count': 126, 'sub_cat_slug': 'meat-alternatives', 'sub_category': 'Meat Alternatives'}, {'category': 'Prepared Foods', 'count': 204, 'sub_cat_slug': 'prepared-meals', 'sub_category': 'Prepared Meals'}, {'category': 'Prepared Foods', 'count': 77, 'sub_cat_slug': 'prepared-soups-salads', 'sub_category': 'Prepared Soups & Salads'}, {'category': 'Pantry Essentials', 'count': 436, 'sub_cat_slug': 'baking', 'sub_category': 'Baking'}, {'category': 'Pantry Essentials', 'count': 146, 'sub_cat_slug': 'canned-goods', 'sub_category': 'Canned Goods'}, {'category': 'Pantry Essentials', 'count': 166, 'sub_cat_slug': 'cereal', 'sub_category': 'Cereal'}, {'category': 'Pantry Essentials', 'count': 461, 'sub_cat_slug': 'condiments-dressings', 'sub_category': 'Condiments & Dressings'}, {'category': 'Pantry Essentials', 'count': 76, 'sub_cat_slug': 'hot-cereal-pancake-mixes', 'sub_category': 'Hot Cereal & Pancake Mixes'}, {'category': 'Pantry Essentials', 'count': 233, 'sub_cat_slug': 'jam-jellies-nut-butters', 'sub_category': 'Jam, Jellies & Nut Butters'}, {'category': 'Pantry Essentials', 'count': 197, 'sub_cat_slug': 'pasta-noodles', 'sub_category': 'Pasta & Noodles'}, {'category': 'Pantry Essentials', 'count': 102, 'sub_cat_slug': 'rice-grains', 'sub_category': 'Rice & Grains'}, {'category': 'Pantry Essentials', 'count': 415, 'sub_cat_slug': 'sauces', 'sub_category': 'Sauces'}, {'category': 'Pantry Essentials', 'count': 229, 'sub_cat_slug': 'soups-broths', 'sub_category': 'Soups & Broths'}, {'category': 'Pantry Essentials', 'count': 362, 'sub_cat_slug': 'spices-seasonings', 'sub_category': 'Spices & Seasonings'}, {'category': 'Breads, Rolls & Bakery', 'count': 143, 'sub_cat_slug': 'breads', 'sub_category': 'Breads'}, {'category': 'Breads, Rolls & Bakery', 'count': 56, 'sub_cat_slug': 'breakfast-bakery', 'sub_category': 'Breakfast Bakery'}, {'category': 'Breads, Rolls & Bakery', 'count': 33, 'sub_cat_slug': 'rolls-buns', 'sub_category': 'Rolls & Buns'}, {'category': 'Breads, Rolls & Bakery', 'count': 46, 'sub_cat_slug': 'tortillas-flat-breads', 'sub_category': 'Tortillas & Flat Breads'}, {'category': 'Desserts', 'count': 43, 'sub_cat_slug': 'cakes-cupcakes', 'sub_category': 'Cakes & Cupcakes'}, {'category': 'Desserts', 'count': 209, 'sub_cat_slug': 'cookies', 'sub_category': 'Cookies'}, {'category': 'Desserts', 'count': 7, 'sub_cat_slug': 'muffins-scones', 'sub_category': 'Muffins & Scones'}, {'category': 'Desserts', 'count': 38, 'sub_cat_slug': 'pastries-brownies-bars', 'sub_category': 'Pastries, Brownies & Bars'}, {'category': 'Desserts', 'count': 9, 'sub_cat_slug': 'pies-tarts', 'sub_category': 'Pies & Tarts'}, {'category': 'Body Care', 'count': 194, 'sub_cat_slug': 'aromatherapy', 'sub_category': 'Aromatherapy'}, {'category': 'Body Care', 'count': 365, 'sub_cat_slug': 'bath-body', 'sub_category': 'Bath & Body'}, {'category': 'Body Care', 'count': 382, 'sub_cat_slug': 'personal-care', 'sub_category': 'Personal Care'}, {'category': 'Supplements', 'count': 19, 'sub_cat_slug': 'childrens-health', 'sub_category': 'Children’s Health'}, {'category': 'Supplements', 'count': 129, 'sub_cat_slug': 'functional-foods', 'sub_category': 'Functional Foods'}, {'category': 'Supplements', 'count': 113, 'sub_cat_slug': 'functional-supplements', 'sub_category': 'Functional Supplements'}, {'category': 'Supplements', 'count': 128, 'sub_cat_slug': 'herbs-homeopathy', 'sub_category': 'Herbs & Homeopathy'}, {'category': 'Supplements', 'count': 426, 'sub_cat_slug': 'specialty-supplements', 'sub_category': 'Specialty Supplements'}, {'category': 'Supplements', 'count': 225, 'sub_cat_slug': 'sports-nutrition-weight-management', 'sub_category': 'Sports Nutrition & Weight Management'}, {'category': 'Supplements', 'count': 374, 'sub_cat_slug': 'vitamins-minerals', 'sub_category': 'Vitamins & Minerals'}, {'category': 'Supplements', 'count': 101, 'sub_cat_slug': 'wellness-seasonal', 'sub_category': 'Wellness & Seasonal'}, {'category': 'Frozen Foods', 'count': 58, 'sub_cat_slug': 'frozen-breakfast', 'sub_category': 'Frozen Breakfast'}, {'category': 'Frozen Foods', 'count': 267, 'sub_cat_slug': 'frozen-entrees-appetizers', 'sub_category': 'Frozen Entrées & Appetizers'}, {'category': 'Frozen Foods', 'count': 108, 'sub_cat_slug': 'frozen-fruits-vegetables', 'sub_category': 'Frozen Fruits & Vegetables'}, {'category': 'Frozen Foods', 'count': 53, 'sub_cat_slug': 'frozen-pizza', 'sub_category': 'Frozen Pizza'}, {'category': 'Frozen Foods', 'count': 289, 'sub_cat_slug': 'ice-cream-frozen-desserts', 'sub_category': 'Ice Cream & Frozen Desserts'}, {'category': 'Snacks, Chips, Salsas & Dips', 'count': 357, 'sub_cat_slug': 'candy-chocolate', 'sub_category': 'Candy & Chocolate'}, {'category': 'Snacks, Chips, Salsas & Dips', 'count': 198, 'sub_cat_slug': 'chips', 'sub_category': 'Chips'}, {'category': 'Snacks, Chips, Salsas & Dips', 'count': 180, 'sub_cat_slug': 'crackers', 'sub_category': 'Crackers'}, {'category': 'Snacks, Chips, Salsas & Dips', 'count': 60, 'sub_cat_slug': 'jerky', 'sub_category': 'Jerky'}, {'category': 'Snacks, Chips, Salsas & Dips', 'count': 425, 'sub_cat_slug': 'nutrition-granola-bars', 'sub_category': 'Nutrition & Granola Bars'}, {'category': 'Snacks, Chips, Salsas & Dips', 'count': 217, 'sub_cat_slug': 'nuts-seeds-dried-fruit', 'sub_category': 'Nuts, Seeds & Dried Fruit'}, {'category': 'Snacks, Chips, Salsas & Dips', 'count': 77, 'sub_cat_slug': 'popcorn-puffs-rice-cakes', 'sub_category': 'Popcorn, Puffs & Rice Cakes'}, {'category': 'Snacks, Chips, Salsas & Dips', 'count': 158, 'sub_cat_slug': 'salsas-dips-spreads', 'sub_category': 'Salsas, Dips & Spreads'}, {'category': 'Seafood', 'count': 53, 'sub_cat_slug': 'fish', 'sub_category': 'Fish'}, {'category': 'Seafood', 'count': 25, 'sub_cat_slug': 'shellfish', 'sub_category': 'Shellfish'}, {'category': 'Beverages', 'count': 328, 'sub_cat_slug': 'coffee', 'sub_category': 'Coffee '}, {'category': 'Beverages', 'count': 338, 'sub_cat_slug': 'juice', 'sub_category': 'Juice'}, {'category': 'Beverages', 'count': 11, 'sub_cat_slug': 'kombucha-tea', 'sub_category': 'Kombucha & Tea'}, {'category': 'Beverages', 'count': 81, 'sub_cat_slug': 'soft-drinks', 'sub_category': 'Soft Drinks'}, {'category': 'Beverages', 'count': 186, 'sub_cat_slug': 'sports-energy-nutritional-drinks', 'sub_category': 'Sports, Energy & Nutritional Drinks'}, {'category': 'Beverages', 'count': 307, 'sub_cat_slug': 'tea', 'sub_category': 'Tea'}, {'category': 'Beverages', 'count': 231, 'sub_cat_slug': 'water-seltzer-sparkling-water', 'sub_category': 'Water, Seltzer & Sparkling Water'}, {'category': 'Wine, Beer & Spirits', 'count': 360, 'sub_cat_slug': 'beer', 'sub_category': 'Beer'}, {'category': 'Wine, Beer & Spirits', 'count': 268, 'sub_cat_slug': 'spirits', 'sub_category': 'Spirits'}, {'category': 'Wine, Beer & Spirits', 'count': 652, 'sub_cat_slug': 'wine', 'sub_category': 'Wine'}, {'category': 'Beauty', 'count': 129, 'sub_cat_slug': 'cosmetics', 'sub_category': 'Cosmetics'}, {'category': 'Beauty', 'count': 321, 'sub_cat_slug': 'facial-care', 'sub_category': 'Facial Care'}, {'category': 'Beauty', 'count': 239, 'sub_cat_slug': 'hair-care', 'sub_category': 'Hair Care'}, {'category': 'Beauty', 'count': 12, 'sub_cat_slug': 'perfume', 'sub_category': 'Perfume'}, {'category': 'Household', 'count': 144, 'sub_cat_slug': 'cleaners', 'sub_category': 'Cleaners'}, {'category': 'Household', 'count': 109, 'sub_cat_slug': 'paper-household-essentials', 'sub_category': 'Paper & Household Essentials'}, {'category': 'Lifestyle', 'count': 14, 'sub_cat_slug': 'apparel-accessories', 'sub_category': 'Apparel & Accessories'}, {'category': 'Lifestyle', 'count': 20, 'sub_cat_slug': 'cards-party', 'sub_category': 'Cards & Party'}, {'category': 'Lifestyle', 'count': 382, 'sub_cat_slug': 'home-kitchen', 'sub_category': 'Home & Kitchen'}, {'category': 'Lifestyle', 'count': 7, 'sub_cat_slug': 'toys-games', 'sub_category': 'Toys & Games'}, {'category': 'Pet', 'count': 11, 'sub_cat_slug': 'accessories', 'sub_category': 'Accessories'}, {'category': 'Pet', 'count': 41, 'sub_cat_slug': 'cat-food', 'sub_category': 'Cat Food'}, {'category': 'Pet', 'count': 107, 'sub_cat_slug': 'dog-food', 'sub_category': 'Dog Food'}]

    def start_requests(self):
        url = 'https://www.wholefoodsmarket.com/api/products/category/fresh-herbs?leafCategory=fresh-herbs&sort=brandaz&limit=60&offset=0'
        yield Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        data_dict = {}
        try:
            data_dict = response.json()
            # brands = data_dict.get('results', [])

        except json.JSONDecodeError as e:
            print('response error :', e)
            a=1

        results = []
        try:
            # Accessing the 'facets' and then the 'refinements' for 'Produce'
            refinements = data_dict.get('facets', [])[0].get('refinements', [])

            results = []
            for category in refinements:
                label = category.get('label', '')
                if 'All Products' in label:
                    continue

                sub_categories = category.get('refinements', [])

                if not sub_categories:
                    record = {
                        'category': label,
                        'sub_cat_slug': category.get('slug', ''),
                        'count': category.get('count', '')
                    }
                    results.append(record)

                for sub_cat in sub_categories:
                    sub_cat_label = sub_cat.get('label', '')
                    sub_cat_slug = sub_cat.get('slug', '')
                    count = sub_cat.get('count', '')

                    # Creating a dictionary for each sub-category and appending it to results
                    record = {
                        'category': label,
                        'sub_category': sub_cat_label,
                        'sub_cat_slug': sub_cat_slug,
                        'count': count
                    }
                    results.append(record)

        except json.JSONDecodeError as e:
            print('Results error :', e)
            a=1

        # total_brands = sum(int(cou['count']) for cou in results if isinstance(cou['count'], (str, int)))

        for sub_cat_result in results:
            count = sub_cat_result.get('count')
            total_pages = ceil(int(count)/60)

            for page_no in range(1 , total_pages):
                offset = int(page_no) * 60
                slug = sub_cat_result.get('sub_cat_slug', '')
                category = sub_cat_result.get('category', '')
                sub_category = sub_cat_result.get('sub_category', '')
                url = f'https://www.wholefoodsmarket.com/api/products/category/{slug}?leafCategory={slug}&sort=brandaz&limit=60&offset={offset}'

                yield Request(url, callback=self.parse_brand_details, dont_filter=True,
                              meta={'category':category, 'sub_category':sub_category, "handle_httpstatus_all": True,})

    def parse_brand_details(self, response):
        if response.status != 200:
            a=1

        try:
            data_dict = response.json()
            brands = data_dict.get('results', [])

            for brand in brands:
                item = OrderedDict()

                slug = brand.get('slug', '')
                brand_name = brand.get('brand', '')
                brand_url = f'https://www.wholefoodsmarket.com/product/{slug}'
                item['Category'] = response.meta.get('category', '')
                item['Sub Category'] = response.meta.get('sub_category', '')
                item['Name'] = brand.get('name', '')
                item['Brand'] = brand.get('brand', '')
                item['Image_Url'] = brand.get('imageThumbnail', '')
                item['Url'] = brand_url

                # yield item
                url = f'https://api.brandfetch.io/v2/search/{brand_name}?limit=5'
                yield Request(url, callback=self.parse_brand_domain, headers=self.headers, dont_filter=True,
                              meta={'item': item, "handle_httpstatus_all": True, })

        except json.JSONDecodeError as e:
            print('error :', e)
            a=1


    def parse_brand_domain(self, response):
        item = response.meta.get('item', {})  # Retrieve the item from the meta

        if response.status == 200:
            try:
                data_dict = response.json()
                # Extract brand domain if available
                brand_name = item.get('Brand', '')
                domains = [res.get('domain', '') for res in data_dict]

                # matching_domain = ''.join([name for name in domains if name.lower().split('.')[0]==brand_name.lower()])
                matching_domain = next(
                                        (name for name in domains if name.lower().split('.')[0] == brand_name.lower()),
                                        domains[0] if domains else ''
                                    )

                # item['Brand Domain'] = matching_domain if matching_domain elif domains[0] if domains else ''
                item['Brand Domain'] = matching_domain
                self.items_scraped += 1
                print('Scraped Items :', self.items_scraped)
                yield item  # Yield the item after adding the brand domain
            except json.JSONDecodeError as e:
                print('Error decoding JSON:', e)
                yield item
        else:
            print('Failed to retrieve brand domain, status code:', response.status)
            yield item