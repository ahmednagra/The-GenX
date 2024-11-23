import csv
import json
import os
import re
from datetime import datetime
from collections import OrderedDict
from math import ceil
from typing import Iterable
from urllib.parse import urljoin, unquote, urlparse, parse_qs

from numpy.core.records import record
from scrapy import signals, Spider, Request, Selector


class WholefoodSpider(Spider):
    name = "wholefood"
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
        self.previous_records = self.read_csv()

    def start_requests(self):
        for record in self.previous_records:
            brand_domain = record.get('Brand Domain', '')
            if not brand_domain:
                record_url = record.get('Url', '')
                brand_name = record.get('Brand', '')
                cleaned_brand_name = re.sub(r'[^a-zA-Z0-9\s]', '', brand_name)
                url = f'https://api.brandfetch.io/v2/search/{cleaned_brand_name}?limit=5'
                yield Request(url, callback=self.parse_brand_domain, headers=self.headers, dont_filter=True,
                              meta={'record_url': record_url, 'brand_name': brand_name, "handle_httpstatus_all": True, })
            else:
                self.write_csv(record=record)
                a=1

    def parse_brand_domain(self, response):
        record_url = response.meta.get('record_url', {})  # Retrieve the item from the meta
        brand_name = response.meta.get('brand_name', {})

        if response.status == 200:
            try:
                data_dict = response.json()
                domains = [res.get('domain', '') for res in data_dict]

                matching_domain = next(
                    (name for name in domains if name.lower().split('.')[0] == brand_name.lower()),
                    domains[0] if domains else ''
                )

                if not matching_domain:
                    row = [rec for rec in self.previous_records if rec.get('Url') == record_url]
                    if row:
                        row = row[0]

                else:
                    # Find the specific record in previous_records and update 'Brand Domain'
                    row = [rec for rec in self.previous_records if rec.get('Url')==record_url]
                    if row:
                        row = row[0]
                        row['Brand Domain'] = matching_domain

                # After updating, write the data back to the CSV
                self.write_csv(record=row)
            except json.JSONDecodeError as e:
                print('Error decoding JSON:', e)

        else:
            print('Failed to retrieve brand domain, status code:', response.status)


    def read_csv(self):
        try:
            # with open('wholefoodsmarket_output/Whole Foods Market Brands Details.csv', 'r', encoding='utf-8') as csv_file:
            with open('wholefoodsmarket_output/updated_Whole Foods Market Brands Details.csv', 'r', encoding='utf-8') as csv_file:
                data = list(csv.DictReader(csv_file))
                return data

        except Exception as e:
            print(f"Error reading the Excel file: {e}")
            return None

    def write_csv(self, record):
        """Write a single record to the CSV file."""
        output_file = 'wholefoodsmarket_output/recent_updated_Whole Foods Market Brands Details.csv'

        try:
            # Check if file exists
            file_exists = os.path.exists(output_file)

            # Open the CSV file in append mode
            with open(output_file, 'a', newline='', encoding='utf-8') as csv_file:
                # Use the record's keys as field names
                fieldnames = record.keys()
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                # Write the header only if the file is new or empty
                if not file_exists or csv_file.tell() == 0:
                    writer.writeheader()

                # Write the single row (record) to the CSV
                writer.writerow(record)

            print(f"Record for '{record['Url']}' written to CSV successfully.")

        except Exception as e:
            print(f"Error writing to the CSV file: {e}")