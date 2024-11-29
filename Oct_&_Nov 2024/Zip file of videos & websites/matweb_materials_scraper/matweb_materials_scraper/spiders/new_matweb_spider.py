import csv
import json
import os
from collections import OrderedDict
from datetime import datetime

from urllib.parse import urljoin

from scrapy import Request, FormRequest, Spider, signals

from .base import BaseSpider


class MatwebSpider(BaseSpider):
    name = "matweb"
    start_urls = ['https://www.matweb.com/search/MaterialGroupSearch.aspx']
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://www.matweb.com',
        'priority': 'u=0, i',
        'referer': 'https://www.matweb.com/search/MaterialGroupSearch.aspx',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    }

    def __init__(self):
        super().__init__()
        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/MatWeb_logs {self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

        self.categories = ['carbon', 'ceramic', 'metal', 'polymer']
        # self.categories = ['carbon']
        self.page_count = 0
        # self.product_count = 0
        self.next_page_value = 0

    def start_requests(self):
        yield from self.spider_idle()

    def parse_homepage(self, response, **kwargs):
        viewstat = response.css('#__VIEWSTATE ::attr("value")').get('')
        viewstat_generator_id = response.css('#__VIEWSTATEGENERATOR ::attr("value")').get('')
        category = response.meta.get('category', '')
        data = self.get_form_data(category, viewstat, viewstat_generator_id, next_page_value=False)
        print('Category :', category)
        yield FormRequest(url=self.start_urls[0], formdata=data, callback=self.parse_category_pagination,
                          dont_filter=True, headers=self.headers, meta={'category': category})

    def parse_category_pagination(self, response):
        category = response.meta.get('category', '')

        if not response.meta.get('next_page', ''):
            total_products = response.css('#ctl00_ContentMain_UcSearchResults1_lblResultCount ::text').get('')
            self.write_logs(f'Category: {category} Found Total Materials : {total_products}')

        materials_urls = response.css('#tblResults a::attr(href)').getall() or []

        for mat_url in materials_urls:
            url = urljoin('https://www.matweb.com/', mat_url)
            yield Request(url, callback=self.parse_detail, headers=self.headers,
                          dont_filter=True, meta=response.meta)

        next_page = response.css('a:contains("Next Page") ::attr(disabled)').get('')
        # when the pagination is end then the disabled property is set in the html response
        if not next_page:
            viewstat = response.css('#__VIEWSTATE ::attr("value")').get('')
            viewstat_generator_id = response.css('#__VIEWSTATEGENERATOR ::attr("value")').get('')
            self.next_page_value += 1

            self.page_count += 1
            print(f'Category: {category} & Page No Called:', self.page_count)
            data = self.get_form_data(category, viewstat, viewstat_generator_id, self.next_page_value)
            yield FormRequest(url=self.start_urls[0], formdata=data, callback=self.parse_category_pagination,
                              dont_filter=True, headers=self.headers,
                              meta={'category': category, 'page_count': self.page_count, 'next_page': True})

    def parse_detail(self, response):
        try:
            item = OrderedDict()
            title = response.css('.tabledataformat.t_ableborder th::text').get('').strip()
            item['Name'] = title
            item['Categories'] = ', '.join(
                response.css('th:contains("Categories:") + td a::text').getall()) or ', '.join(
                response.css('#ctl00_ContentMain_ucDataSheet1_trMatlGroups td a ::text').getall())
            item['Key Words'] = ', '.join(
                response.css('#ctl00_ContentMain_ucDataSheet1_trMatlNotes td ::text').getall())
            item['Vendors'] = self.get_vendor(response)
            item['Color'] = ''
            item['Crystal Structure'] = ''
            item['Physical Properties'] = self.get_properties(response, 'Physical')
            item['Chemical Properties'] = self.get_properties(response, 'Chemical')
            item['Mechanical Properties'] = self.get_properties(response, 'Mechanical')
            item['Electrical Properties'] = self.get_properties(response, 'Electrical')
            item['Thermal Properties'] = self.get_properties(response, 'Thermal')
            item['Optical Properties'] = self.get_properties(response, 'Optical')
            item['Component Elements Properties'] = self.get_properties(response, 'Component')
            item['Descriptive Properties'] = self.get_properties(response, 'Descriptive')
            item['Processing Properties'] = self.get_properties(response, 'Processing')
            item['URL'] = response.url

            item['Category'] = response.meta.get('category', '')
            page_count = response.meta.get('page_count', '')
            page_count = str(int(page_count) + 1) if page_count else '1'
            item['Page NO'] = f'Category: {item['Category']} Page No: {page_count}'

            # self.product_count += 1
            # print('Total Products are scraped:', self.product_count)
            # yield item
            self.write_csv(record=item)

        except Exception as e:
            self.write_logs(f'Error in item write URL:{response.url} Error:{e}')

    def get_form_data(self, category, viewstat, viewstat_generator_id, next_page_value):
        # Default `viewstat_generator_id` if not provided
        viewstat_generator_id = viewstat_generator_id if viewstat_generator_id else "640EFF8C"

        # Base data dictionary
        data = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            'ctl00_ContentMain_ucMatGroupTree_msTreeView_ExpandState': 'ccccccnc',
            'ctl00_ContentMain_ucMatGroupTree_msTreeView_SelectedNode': '',
            'ctl00_ContentMain_ucMatGroupTree_msTreeView_PopulateLog': '',
            '__VIEWSTATE': viewstat,
            "__VIEWSTATEGENERATOR": viewstat_generator_id,
            'ctl00$txtQuickText': '',
            'ctl00$ContentMain$ucPopupMessage1$hndPopupControl': '',
            'ctl00$ContentMain$UcMatGroupFinder1$txtSearchText': '',
            'ctl00$ContentMain$UcMatGroupFinder1$TextBoxWatermarkExtender2_ClientState': '',
        }

        # Define payload based on category
        payload = {}
        if category == 'carbon':
            payload = {
                'ctl00$ContentMain$txtMatGroupID': '283',
                'ctl00$ContentMain$txtMatGroupText': 'Carbon (866 matls)',
                'ctl00$ContentMain$btnSubmit.x': '15',
                'ctl00$ContentMain$btnSubmit.y': '13',
            }
        elif category == 'ceramic':
            payload = {
                'ctl00$ContentMain$txtMatGroupID': '11',
                'ctl00$ContentMain$txtMatGroupText': 'Ceramic (10004 matls)',
                'ctl00$ContentMain$btnSubmit.x': '15',
                'ctl00$ContentMain$btnSubmit.y': '14',
            }
        elif category == 'metal':
            payload = {
                'ctl00$ContentMain$txtMatGroupID': '9',
                'ctl00$ContentMain$txtMatGroupText': 'Metal (17052 matls)',
                'ctl00$ContentMain$btnSubmit.x': '27',
                'ctl00$ContentMain$btnSubmit.y': '12',
            }
        elif category == 'polymer':
            payload = {
                'ctl00$ContentMain$txtMatGroupID': '10',
                'ctl00$ContentMain$txtMatGroupText': 'Polymer (97635 matls)',
                'ctl00$ContentMain$btnSubmit.x': '30',
                'ctl00$ContentMain$btnSubmit.y': '10',
            }

        # Merge payload into data if category is specified
        if payload:
            data.update(payload)

        # Define and incorporate next_page data if `next_page` is True
        if next_page_value:
            page_size2 = '50' if next_page_value == 1 else '200'
            next_page_data = {
                '__EVENTTARGET': 'ctl00$ContentMain$UcSearchResults1$lnkNextPage',
                # 'ctl00$ContentMain$UcSearchResults1$drpPageSize1': '200',
                # 'ctl00$ContentMain$UcSearchResults1$drpFolderList': '0',
                # 'ctl00$ContentMain$UcSearchResults1$txtFolderMatCount': '0/0',
                # 'ctl00$ContentMain$UcSearchResults1$drpPageSelect2': '1',
                # 'ctl00$ContentMain$UcSearchResults1$drpPageSize2': '50',
                'ctl00$ContentMain$UcSearchResults1$drpPageSelect1': str(next_page_value),
                'ctl00$ContentMain$UcSearchResults1$drpPageSize1': '200',
                'ctl00$ContentMain$UcSearchResults1$drpFolderList': '0',
                'ctl00$ContentMain$UcSearchResults1$txtFolderMatCount': '0/0',
                'ctl00$ContentMain$UcSearchResults1$drpPageSelect2': str(next_page_value),
                # 'ctl00$ContentMain$UcSearchResults1$drpPageSize2': '200',
                'ctl00$ContentMain$UcSearchResults1$drpPageSize2': page_size2,
            }
            data.update(next_page_data)

            # Remove specific keys after updating with `next_page_data`
            data.pop('ctl00$ContentMain$btnSubmit.x', None)
            data.pop('ctl00$ContentMain$btnSubmit.y', None)

        return data

    def get_physical_pro(self, response):
        keys = [key.strip() for key in response.css('tr:contains("Physical Properties") th ::text').getall()[1:]]
        values = []
        records = response.css('tr:contains("Physical Properties") + tr td')
        for record in records:
            values.append(''.join(record.css('::text').getall()).strip())

        # Separate the main key and the remaining values
        main_key = values[0]  # The first item in values is the main key
        remaining_values = values[1:]  # The rest of the values

        # Create the list of dictionaries, ensuring everything is stripped
        physical_properties = [
            {
                main_key: {
                    keys[0]: remaining_values[0],
                    keys[1]: remaining_values[1],
                    keys[2]: remaining_values[2],
                }
            }
        ]

        return physical_properties

    def get_properties(self, response, value):
        try:
            # value = 'Processing'
            keys = [key.strip() for key in response.css(f'tr:contains("{value} Properties") th ::text').getall()[1:]]
            keys = keys or response.css(f'tr:contains("{value}") th::text').getall()
            values = []

            # Extract records after the specified properties row
            records = response.css(f'tr:contains("{value} Properties") + .datarowSeparator td')
            records = records or response.css(f'tr:contains("{value}") + .datarowSeparator td')
            records = records or response.css(f'tr:contains("{value} Properties") ~ tr')[:5]

            if not keys:
                # If no keys are found, process the records
                for record in records:
                    rec_row = record.css('td ::text').getall()
                    if rec_row != '\xa0' and rec_row:
                        values.append(rec_row)

                # Flatten the list of values and join them with newlines
                flattened_values = [' '.join(record) for record in values]
                return '\n '.join(flattened_values) if flattened_values else 'N/A'

            else:
                # If keys are found, process records and append to values
                list_record = []
                for record in records:
                    rec_row = ''.join([text.strip() for text in record.css('td ::text').getall() if text.strip()])
                    if rec_row != '\xa0' and rec_row:
                        list_record.append(rec_row)

                values.append(list_record)

            # Create the list of dictionaries from the values
            property_dicts = []
            for record in values:
                # Separate the main key and the remaining values
                main_key = record[0] if record else ""  # The first item in the record is the main key
                remaining_values = record[1:]  # The rest of the values

                # Create the dictionary for the current record
                property_dict = {
                    main_key: {
                        keys[i]: remaining_values[i] if i < len(remaining_values) else None
                        for i in range(len(keys))
                    }
                }

                # Append the dictionary to the list
                property_dicts.append(property_dict)

            return property_dicts if property_dicts else 'N/A'

        except Exception as e:
            # Catch any exception and return 'N/A'
            print(f"Error in get_properties: {e}")  # Optionally log the error for debugging
            return 'N/A'

    def get_vendor(self, response):
        vendors_text = response.css('tr:contains("Vendors") td ::text').getall()
        if any("No vendors are listed for this" in text for text in vendors_text):
            return 'N/A'
        else:
            text = ' '.join(vendors_text).split('Click here  to view all')[0]

            return text

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MatwebSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        if self.categories:
            self.next_page_value = 0
            category = self.categories.pop()
            self.write_logs(f"\n\n Category:{category} now start scraping")

            req = Request(url=self.start_urls[0],
                          callback=self.parse_homepage,
                          dont_filter=True,
                          headers=self.headers,
                          meta={'handle_httpstatus_all': True, 'category': category})

            try:
                self.crawler.engine.crawl(req)  # For latest Python version
            except TypeError:
                self.crawler.engine.crawl(req, self)  # For old Python version < 10

            # yield req
