import csv
import json
import os
from collections import OrderedDict
from datetime import datetime
from time import sleep

from urllib.parse import urljoin, urlencode

import requests
from scrapy import Selector
from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions

from scrapy import Request, FormRequest, Spider, signals

from .base import BaseSpider


class ASMSpider(BaseSpider):
    name = "asm"
    start_urls = ['https://eportal.asminternational.org/myasm/s/login/']

    login_headers = {
        'accept': '*/*',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'renderCtx=%7B%22pageId%22%3A%221f0073e2-88a4-4859-985e-820f1625353d%22%2C%22schema%22%3A%22Published%22%2C%22viewType%22%3A%22Published%22%2C%22brandingSetId%22%3A%22d8d676ed-f9db-4a0c-8cc8-e83dccd6d02f%22%2C%22audienceIds%22%3A%22%22%7D; _ga=GA1.1.1729744823.1732119154; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1; hum_asmi_visitor=1e45f85b-eb12-4c54-9533-230609c21fd6; hum_asmi_synced=true; ajs_anonymous_id=%2226a3a6d3-0c2a-4299-af6c-90e81302cd3b%22; oinfo=c3RhdHVzPUFDVElWRSZ0eXBlPTYmb2lkPTAwRDQ2MDAwMDAwWmJYSQ==; autocomplete=1; oid=00D46000000ZbXI; _pendo_accountId.e3cfc096-605e-4f72-6226-04a1fe5fb047=00D46000000ZbXIEA0; _pendo_visitorId.e3cfc096-605e-4f72-6226-04a1fe5fb047=005Vm000002kLGPIA2; _pendo_meta.e3cfc096-605e-4f72-6226-04a1fe5fb047=3280141792; sbjs_migrations=1418474375998%3D1; sbjs_current_add=fd%3D2024-11-27%2015%3A00%3A01%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.asminternational.org%2F%7C%7C%7Crf%3D%28none%29; sbjs_first_add=fd%3D2024-11-27%2015%3A00%3A01%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.asminternational.org%2F%7C%7C%7Crf%3D%28none%29; sbjs_current=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29; sbjs_first=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29; sbjs_udata=vst%3D1%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F131.0.0.0%20Safari%2F537.36; sbjs_session=pgs%3D1%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fwww.asminternational.org%2F; _ga_4VS3CFEZWG=GS1.1.1732719603.6.1.1732719612.0.0.0; _ga_20WSV7567T=GS1.1.1732719613.5.0.1732719613.0.0.0',
        'origin': 'https://eportal.asminternational.org',
        'priority': 'u=1, i',
        # 'referer': 'https://eportal.asminternational.org/myasm/s/login/?ec=302&inst=Vm&startURL=%2Fmyasm%2Fidp%2Flogin%3Fapp%3D0sp4R000000KzAU%26_gl%3D1*1dk2gy6*_ga*MTcyOTc0NDgyMy4xNzMyMTE5MTU0*_ga_4VS3CFEZWG*MTczMjcxOTYwMy42LjAuMTczMjcxOTYwMy4wLjAuMA..',
        'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        # 'x-sfdc-page-scope-id': 'b4a9ef82-e069-462a-b1ab-14d0c2a042e3',
    }

    def __init__(self):
        super().__init__()
        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_logs {self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

        self.page_count = 0
        self.next_page_value = 0

        # edge_options = Options()
        # edge_options.use_chromium = True  # Use Chromium-based Edge
        # msedgedriver_path = r'input\msedgedriver.exe'
        # edge_options = EdgeOptions()
        # edge_service = EdgeService(msedgedriver_path)
        # self.driver = webdriver.Edge(service=edge_service, options=edge_options)
        # self.driver = webdriver.ChromiumEdge(service=ChromiumService(EdgeChromiumDriverManager().install()))
        # self.driver.maximize_window()

    def start_requests(self):
        # login= self.login()
        # headers = {
        #     'Accept': 'application/json, text/plain, */*',
        #     'Accept-Language': 'en-US,en;q=0.9',
        #     'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjVDNkExQzcwOUNBQzE2M0FENDk3OTQ3Qjg1QjQ5ODkxQ0MzRDg0QkFSUzI1NiIsInR5cCI6IkpXVCIsIng1dCI6IlhHb2NjSnlzRmpyVWw1UjdoYlNZa2N3OWhMbyJ9.eyJuYmYiOjE3MzI3MjQ5NTcsImV4cCI6MTczMjc2MDk1NywiaXNzIjoiaHR0cHM6Ly9nbXBpZGVudGl0eS5hc21pbnRlcm5hdGlvbmFsLm9yZyIsImF1ZCI6InNwYSIsIm5vbmNlIjoiWVhKdk0yMXVUREprUkhKa2JVRjFkMHRITkZoUVEzbEtjV1I1V0hwbE5HODJNMTlMWTNsRVRUSlBTMk5mIiwiaWF0IjoxNzMyNzI0OTU3LCJhdF9oYXNoIjoiRkpUWS1LdjJvdEx3WEtLRjlubVNsQSIsInNfaGFzaCI6IllzSjh0WWJxUHVZaFota09vWDFBQ1EiLCJzaWQiOiJBNDI4MjI2RjUxNDc2NjVBODRBM0U1REU3MjdBNjY3MiIsInN1YiI6IjE1NDE5MjE2MTEyNzExMjAyNDI3MjkxNzA0MjMzT1FOUTNOTjQxWVVfMSIsImF1dGhfdGltZSI6MTczMjcyNDk0NSwiaWRwIjoibG9jYWwiLCJGdWxsTmFtZSI6IkphdmFkIFNoaXJhbmkiLCJVc2VyVHlwZSI6IjEiLCJIYXNDb3Jwb3JhdGVVc2VySWQiOiJUcnVlIiwiSXNTaW5nbGVVc2VyIjoiRmFsc2UiLCJDb21wYW55IjoiU2hpcmFuaSBIb3VzZWhvbGQgQy0wNjI5MjQxOCIsIkVtYWlsIjoianNoaXJhbmk5NkBnbWFpbC5jb20iLCJTdWJzY3JpcHRpb25EYXRlIjoiMTEvMjcvMjAyNSIsIkNvcmUiOiJSfDB8MCIsIlNtYXJ0IENvbXAiOiJSfDB8NSIsIkV4dGVuZGVkIFJhbmdlIjoiUnw0fDUiLCJTdXBwbGllcnMiOiJSfDB8MCIsIlBvbHltZXJzUExVUyI6IlJ8MHwwIiwiZVhwb3J0ZXIiOiJSfDB8MTAwIiwiVHJhY2tlciI6IlJ8MHwwIiwiRGF0YVBMVVMiOiJSfDB8MCIsIkVudmlybyI6IlJ8MHwwIiwiQ29tcGxpYW5jZSI6IlJ8Mnw1IiwiTWF0ZXJpYWxDb25zb2xlIjoiUnwwfDUiLCJCT00iOiJYfC0xfC0xIiwiQk9NIFZpZGVvIG9ubHkiOiJYfC0xfC0xIiwiUHJlZGljdG9yIjoiUnwwfDE1IiwiTENBIjoiWHwtMXwtMSIsIkxDQSBWaWRlbyBvbmx5IjoiWHwtMXwtMSIsIkRvY3VtZW50cyI6IlJ8MHwwIiwiYW1yIjpbInB3ZCJdfQ.eB-nvgPXqqWpZ80osyXKpfwliHJM2NgBo4cHnuWJIWr5cOMaC5W_1BOHbl21EDU1coxOCJrLoV_BpB_2uNjUB-eyo31FP_1gS9CE_qlIi0Ys6XNUr7Ab-dgxdI8nFpBT1Pj1TwMe8HkA1_5qFV7jzMVJKfMVJOxu16K_lnc8H8Z3Qd65OHoopm-rIOteoUZMK0DKh-iqvUDDduEgRjAl-2-JLD4w8GATP_z3JTfbseigpAGKd-r1ZtxMPwKrqte33_zixrrgQsj2_qjVN7FWGp8EaAT8u6tsi0QFCVH_b39u7xUsQZO5LAGAGd7OTYpvsgIwVyA_gmXX4r8GRQV8uA',
        #     'Connection': 'keep-alive',
        #     'Origin': 'https://gmppro.asminternational.org',
        #     'Referer': 'https://gmppro.asminternational.org/',
        #     'Sec-Fetch-Dest': 'empty',
        #     'Sec-Fetch-Mode': 'cors',
        #     'Sec-Fetch-Site': 'cross-site',
        #     'UnitSystem': '0',
        #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        #     'ValueReturnMode': 'ActualAndFormattedValue',
        #     'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        #     'sec-ch-ua-mobile': '?0',
        #     'sec-ch-ua-platform': '"Windows"',
        # }
        self.login_headers['headers'] = 'application/json, text/plain, */*'
        self.login_headers['Authorization'] = 'Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjVDNkExQzcwOUNBQzE2M0FENDk3OTQ3Qjg1QjQ5ODkxQ0MzRDg0QkFSUzI1NiIsInR5cCI6IkpXVCIsIng1dCI6IlhHb2NjSnlzRmpyVWw1UjdoYlNZa2N3OWhMbyJ9.eyJuYmYiOjE3MzI3MjQ5NTcsImV4cCI6MTczMjc2MDk1NywiaXNzIjoiaHR0cHM6Ly9nbXBpZGVudGl0eS5hc21pbnRlcm5hdGlvbmFsLm9yZyIsImF1ZCI6InNwYSIsIm5vbmNlIjoiWVhKdk0yMXVUREprUkhKa2JVRjFkMHRITkZoUVEzbEtjV1I1V0hwbE5HODJNMTlMWTNsRVRUSlBTMk5mIiwiaWF0IjoxNzMyNzI0OTU3LCJhdF9oYXNoIjoiRkpUWS1LdjJvdEx3WEtLRjlubVNsQSIsInNfaGFzaCI6IllzSjh0WWJxUHVZaFota09vWDFBQ1EiLCJzaWQiOiJBNDI4MjI2RjUxNDc2NjVBODRBM0U1REU3MjdBNjY3MiIsInN1YiI6IjE1NDE5MjE2MTEyNzExMjAyNDI3MjkxNzA0MjMzT1FOUTNOTjQxWVVfMSIsImF1dGhfdGltZSI6MTczMjcyNDk0NSwiaWRwIjoibG9jYWwiLCJGdWxsTmFtZSI6IkphdmFkIFNoaXJhbmkiLCJVc2VyVHlwZSI6IjEiLCJIYXNDb3Jwb3JhdGVVc2VySWQiOiJUcnVlIiwiSXNTaW5nbGVVc2VyIjoiRmFsc2UiLCJDb21wYW55IjoiU2hpcmFuaSBIb3VzZWhvbGQgQy0wNjI5MjQxOCIsIkVtYWlsIjoianNoaXJhbmk5NkBnbWFpbC5jb20iLCJTdWJzY3JpcHRpb25EYXRlIjoiMTEvMjcvMjAyNSIsIkNvcmUiOiJSfDB8MCIsIlNtYXJ0IENvbXAiOiJSfDB8NSIsIkV4dGVuZGVkIFJhbmdlIjoiUnw0fDUiLCJTdXBwbGllcnMiOiJSfDB8MCIsIlBvbHltZXJzUExVUyI6IlJ8MHwwIiwiZVhwb3J0ZXIiOiJSfDB8MTAwIiwiVHJhY2tlciI6IlJ8MHwwIiwiRGF0YVBMVVMiOiJSfDB8MCIsIkVudmlybyI6IlJ8MHwwIiwiQ29tcGxpYW5jZSI6IlJ8Mnw1IiwiTWF0ZXJpYWxDb25zb2xlIjoiUnwwfDUiLCJCT00iOiJYfC0xfC0xIiwiQk9NIFZpZGVvIG9ubHkiOiJYfC0xfC0xIiwiUHJlZGljdG9yIjoiUnwwfDE1IiwiTENBIjoiWHwtMXwtMSIsIkxDQSBWaWRlbyBvbmx5IjoiWHwtMXwtMSIsIkRvY3VtZW50cyI6IlJ8MHwwIiwiYW1yIjpbInB3ZCJdfQ.eB-nvgPXqqWpZ80osyXKpfwliHJM2NgBo4cHnuWJIWr5cOMaC5W_1BOHbl21EDU1coxOCJrLoV_BpB_2uNjUB-eyo31FP_1gS9CE_qlIi0Ys6XNUr7Ab-dgxdI8nFpBT1Pj1TwMe8HkA1_5qFV7jzMVJKfMVJOxu16K_lnc8H8Z3Qd65OHoopm-rIOteoUZMK0DKh-iqvUDDduEgRjAl-2-JLD4w8GATP_z3JTfbseigpAGKd-r1ZtxMPwKrqte33_zixrrgQsj2_qjVN7FWGp8EaAT8u6tsi0QFCVH_b39u7xUsQZO5LAGAGd7OTYpvsgIwVyA_gmXX4r8GRQV8uA'
        self.login_headers['Origin'] = 'https://gmppro.asminternational.org'
        self.login_headers['Referer'] = 'https://gmppro.asminternational.org/'
        self.login_headers['ValueReturnMode'] = 'ActualAndFormattedValue'

        res = requests.get('https://webapp-tmasm-refdata.azurewebsites.net/en/material-groups', headers=self.login_headers)
        categories = [{'id': cat['id'], 'name': cat['name']} for cat in res.json()]

        for cat in categories:
            cat_id = cat.get('id', '')
            cat_name = cat.get('name', '')

            if not cat_id or not cat_name:
                pass

            data = self.get_form_data(cat_id, cat_name)
            res = requests.post(
                'https://webapp-tmasm-refdata.azurewebsites.net/en/total-search/quick-search',
                headers=self.login_headers,
                json=data,
            )

            a=1
            # Initiate the POST request
            # yield FormRequest(url='https://webapp-tmasm-refdata.azurewebsites.net/en/total-search/quick-search',
            #     method='POST', body=json.dumps(data), headers=self.login_headers, callback=self.parse, meta={'handle_httpstatus_all': True}
            # )

    def parse(self, response):
        a=1

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

    def get_form_data(self, cat_id, cat_name, viewstat_generator_id=False, next_page_value=False):
        # Base data dictionary
        json_data = {
            'commonSearchType': 2,
            'searchTerm': '',
            'standardsList': [],
            'materialGroups': [
                {
                    'nameAdditionalContent': '',
                    # 'name': 'Wood',
                    'name': cat_name,
                    # 'id': 9,
                    'id': cat_id,
                    'isPolymer': False,
                    'isBiopolymer': False,
                    'show': True,
                    'level': 0,
                    'expandable': True,
                    'tmMaterialGroupId': None,
                },
            ],
            'showAll': False,
        }

        # Define and incorporate next_page data if `next_page` is True
        if next_page_value:
            page_size2 = '50' if next_page_value == 1 else '200'
            next_page_data = {
                '__EVENTTARGET': 'ctl00$ContentMain$UcSearchResults1$lnkNextPage',
                'ctl00$ContentMain$UcSearchResults1$drpPageSelect1': str(next_page_value),
                'ctl00$ContentMain$UcSearchResults1$drpPageSize1': '200',
                'ctl00$ContentMain$UcSearchResults1$drpFolderList': '0',
                'ctl00$ContentMain$UcSearchResults1$txtFolderMatCount': '0/0',
                'ctl00$ContentMain$UcSearchResults1$drpPageSelect2': str(next_page_value),
                'ctl00$ContentMain$UcSearchResults1$drpPageSize2': page_size2,
            }
            json_data.update(next_page_data)

            # Remove specific keys after updating with `next_page_data`
            json_data.pop('ctl00$ContentMain$btnSubmit.x', None)
            json_data.pop('ctl00$ContentMain$btnSubmit.y', None)

        return json_data

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

    def login(self):
        try:
            self.driver.get(self.start_urls[0])
            sleep(3)
            username= 'jshirani96@gmail.com'
            password = 'NERGYAI2024'

            # Selectors
            input_username_selector = '#sfdc_username_container input'
            input_password_selector = '#sfdc_password_container input'
            login_button_selector = 'button.slds-button--brand.loginButton'

            # Locate and interact with the username field
            input_username_field = self.driver.find_element(By.CSS_SELECTOR, input_username_selector)
            input_username_field.send_keys(username)
            sleep(1)

            # Locate and interact with the password field
            input_password_field = self.driver.find_element(By.CSS_SELECTOR, input_password_selector)
            input_password_field.send_keys(password)
            sleep(1)

            # Locate and click the login button
            login_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, login_button_selector))
            )
            login_button.click()

            # Wait until the profile page is fully loaded
            WebDriverWait(self.driver, 30).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            WebDriverWait(self.driver, 30).until(
                EC.url_contains("org/myasm/s/")  # Update "profile" with a unique part of the profile page URL
            )

            text = 'Javad Shirani'

            # Wait until the text appears in the page source
            profile_page_identifier = WebDriverWait(self.driver, 30).until(
                lambda driver: text in driver.page_source
            )

            # Wait for "My Data Ecosystem" link and click it
            my_data_ecosystem_link = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[title="My Data Ecosystem"]'))
            )
            my_data_ecosystem_link.click()
            # Wait until the profile page is fully loaded
            WebDriverWait(self.driver, 30).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )

            # Wait for the "Open" button to be clickable and then click it
            open_button = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.my-content-button[href*="sso.totalmateria.com"]'))
            )
            open_button.click()
            # Wait until the profile page is fully loaded
            WebDriverWait(self.driver, 30).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            sleep(15)
            # Get the current window handles (tabs)
            original_window = self.driver.current_window_handle

            # Wait for the new tab to open (there should be more than one window handle now)
            WebDriverWait(self.driver, 10).until(EC.number_of_windows_to_be(2))

            # Switch to the new tab (the second window)
            for window_handle in self.driver.window_handles:
                if window_handle != original_window:
                    self.driver.switch_to.window(window_handle)
                    break

            sleep(5)
            # Now you're in the new tab, and you can perform actions here
            print('Successfully switched to the new tab and can perform actions.')
        except Exception as e:
            print('Error:', e)

    # @classmethod
    # def from_crawler(cls, crawler, *args, **kwargs):
    #     spider = super(ASMSpider, cls).from_crawler(crawler, *args, **kwargs)
    #     crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
    #     return spider
    #
    # def spider_idle(self):
    #     if self.categories:
    #         self.page_count = 0
    #         self.next_page_value = 0
    #         category = self.categories.pop()
    #         self.write_logs(f"\n\n Category:{category} now start scraping")
    #
    #         req = Request(url=self.start_urls[0],
    #                       callback=self.parse_homepage,
    #                       dont_filter=True,
    #                       headers=self.headers,
    #                       meta={'handle_httpstatus_all': True, 'category': category})
    #
    #         try:
    #             self.crawler.engine.crawl(req)  # For latest Python version
    #         except TypeError:
    #             self.crawler.engine.crawl(req, self)  # For old Python version < 10
    #
    #
    #
