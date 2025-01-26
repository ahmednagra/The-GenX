import os
from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict

from scrapy import Request, FormRequest, signals

from .base import BaseSpider


class MatwebSpider(BaseSpider):
    name = "matweb"
    custom_settings = {
        "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        'DOWNLOADER_MIDDLEWARES': {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
            "scrapy_poet.InjectionMiddleware": 543,  # Add this line

            # Add Utf8ResponseMiddleware with an appropriate priority
            'matweb_materials_scraper.middlewares.Utf8ResponseMiddleware': 543,
        },
        'REQUEST_FINGERPRINTER_CLASS': "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        'ZYTE_API_KEY': "905eefc5007b4c5b86a06cb416a0061d",
        "ZYTE_API_TRANSPARENT_MODE": True,
    }
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
        self.page_count = 0
        self.next_page_value = 0
        # self.categories = ['carbon', 'ceramic', 'metal', 'polymer']
        self.categories = ['polymer']

        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/MatWeb_logs {self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def start_requests(self):
        yield from self.spider_idle()

    def parse_homepage(self, response, **kwargs):
        viewstat = response.css('#__VIEWSTATE ::attr("value")').get('')
        viewstat_generator_id = response.css('#__VIEWSTATEGENERATOR ::attr("value")').get('')
        category = response.meta.get('category', '')
        # data = self.get_form_data(category, viewstat, viewstat_generator_id, next_page_value=False)
        print('Category :', category)

        # yield FormRequest(url=self.start_urls[0], formdata=data, callback=self.parse_category_pagination,
        #                   dont_filter=True, headers=self.headers, meta={'category': category})

        # code for sub categories
        # Extract values from the href attribute of the first <a> tag
        table = response.css(f'table[cellpadding="0"]:contains("{category.title()}")')
        # href_value = response.css('a#ctl00_ContentMain_ucMatGroupTree_msTreeViewn1::attr(href)').get('')
        href_value = table.css('a::attr(href)').get('')

        # Extract the values needed for __CALLBACKPARAM
        callback_params = href_value.split(",")[-5:]
        callback_params = [param.strip("'") for param in callback_params]

        # Construct the __CALLBACKPARAM value
        __CALLBACKPARAM = "|".join(callback_params)
        subcategory = "|".join(callback_params)

        data = self.get_form_data(category, viewstat, viewstat_generator_id, next_page_value=False, subcategory=subcategory)

        yield FormRequest(url=self.start_urls[0], formdata=data, callback=self.parse_sub_categories,
                          dont_filter=True, headers=self.headers, meta={'category': category})
    def parse_sub_categories(self, response):
        a=1

    def parse_category_pagination(self, response):
        category = response.meta.get('category', '')
        page_no = response.meta.get('next_page', '')
        # if not response.meta.get('next_page', ''):
        if not page_no:
            total_products = response.css('#ctl00_ContentMain_UcSearchResults1_lblResultCount ::text').get('')
            self.write_logs(f'Category: {category} Found Total Materials : {total_products}')

        materials_urls = response.css('#tblResults a::attr(href)').getall() or []
        print(f'Category: {category}, Page No: {page_no} Total Urls : {len(materials_urls)}')

        # materials_text = response.css('#tblResults a::text').getall()[1:] or []
        # materials = zip(materials_text, materials_urls)
        # materials_list = list(materials)  # Convert zip to a list
        # # print(materials_list)
        #
        # # # Iterate over the materials with name and URL
        # # for name, mat_url in materials_list[0:20]:
        # #     print(f"Material Name: {name}")
        # #     print(f"Material URL: {mat_url}")
        for mat_url in materials_urls[0:5]:
            url = urljoin('https://www.matweb.com/', mat_url)
            # yield Request(url, callback=self.parse_detail, headers=self.headers,
            #               dont_filter=True, meta=response.meta)

        next_page = response.css('a:contains("Next Page") ::attr(disabled)').get('')

        # when the pagination is end then the disabled property is set in the html response
        if not next_page:
            viewstat = response.css('#__VIEWSTATE ::attr("value")').get('')
            viewstat_generator_id = response.css('#__VIEWSTATEGENERATOR ::attr("value")').get('')

            self.next_page_value += 1
            self.page_count += 1
            # print(f'Category: {category} & Page No Called:', self.page_count)

            data = self.get_form_data(category, viewstat, viewstat_generator_id, self.next_page_value)
            yield FormRequest(url=self.start_urls[0], formdata=data, callback=self.parse_category_pagination,
                              dont_filter=True, headers=self.headers,
                              meta={'category': category, 'page_count': self.page_count, 'next_page': True})

    def parse_detail(self, response):
        if 'https://www.matweb.com/errorUser.aspx?msgid=8' in response.url:
            a=1

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
            item['Physical Properties'] = self.get_properties(response, 'Physical Properties')
            item['Chemical Properties'] = self.get_properties(response, 'Chemical Properties')
            item['Mechanical Properties'] = self.get_properties(response, 'Mechanical Properties')
            item['Electrical Properties'] = self.get_properties(response, 'Electrical Properties')
            item['Thermal Properties'] = self.get_properties(response, 'Thermal Properties')
            item['Optical Properties'] = self.get_properties(response, 'Optical Properties')
            item['Component Elements Properties'] = self.get_properties(response, 'Component Properties')
            item['Descriptive Properties'] = self.get_properties(response, 'Descriptive Properties')
            item['Processing Properties'] = self.get_properties(response, 'Processing Properties')
            item['Chemical Resistance Properties'] = self.get_properties(response, 'Chemical Resistance Properties')
            item['URL'] = response.url

            # item['Category'] = response.meta.get('category', '')
            # page_count = response.meta.get('page_count', '')
            # page_count = str(int(page_count) + 1) if page_count else '1'
            # item['Page NO'] = f'Category: {item['Category']} Page No: {page_count}'

            # self.write_csv(record=item)
            self.write_json(record=item)
        except Exception as e:
            self.write_logs(f'Error in item write URL:{response.url} Error:{e}')

    def get_form_data(self, category, viewstat, viewstat_generator_id, next_page_value, subcategory):
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
        elif subcategory:
            payload= {
                'ctl00$ContentMain$txtMatGroupID': '',
                'ctl00$ContentMain$txtMatGroupText': '',
                '__CALLBACKID': 'ctl00$ContentMain$ucMatGroupTree$msTreeView', #
                # '__CALLBACKPARAM': '1|118|fff|21|Ceramic (10004 matls)0|11'
                '__CALLBACKPARAM': subcategory
            }

        # Merge payload into data if category is specified
        if payload:
            data.update(payload)

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
            data.update(next_page_data)

            # Remove specific keys after updating with `next_page_data`
            data.pop('ctl00$ContentMain$btnSubmit.x', None)
            data.pop('ctl00$ContentMain$btnSubmit.y', None)

        return data

    def get_properties(self, response, value):
        try:
            # Extract the headers (keys)
            keys = [
                       key.strip() for key in response.css(f'tr:contains("{value} Properties") th ::text').getall()[1:]
                   ] or response.css(f'tr:contains("{value}") th::text').getall()[1:]

            if not keys:
                return 'N/A'  # Return early if no keys are found

            # Extract all rows following the specified property section
            # rows = response.css(f'tr:contains("{value} Properties") ~ tr.datarowSeparator')
            rows = response.css(f'tr:contains("{value} Properties") ~ tr')
            if not rows:
                # rows = response.css(f'tr:contains("{value}") ~ tr.datarowSeparator')
                rows = response.css(f'tr:contains("{value}") ~ tr')

            # Process each row and extract the data
            property_dicts = []
            for row in rows:
                cells = row.css('td')
                if 'colspan="4"' in cells.get(''):
                    break

                if cells:
                    # Extract property name and corresponding values
                    main_key = cells[0].css('::text').get('').strip()  # First column is the property name
                    remaining_values = [
                        ''.join(cell.css('::text').getall()).strip() for cell in cells[1:]
                    ]  # Remaining columns

                    # Map the extracted values to the keys
                    property_dict = {
                        main_key: {
                            keys[i]: remaining_values[i] if i < len(remaining_values) else None
                            for i in range(len(keys))
                        }
                    }

                    # Add the dictionary to the results list
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
            self.page_count = 0
            self.next_page_value = 0
            category = self.categories.pop()
            self.write_logs(f"\n\n Category:{category} now start scraping")

            req = Request(url=self.start_urls[0],
                          callback=self.parse_homepage,
                          dont_filter=True,
                          headers=self.headers,
                          meta={'handle_httpstatus_all': True, 'category': category})

            try:
                self.crawler.engine.crawl(req)
            except TypeError:
                self.crawler.engine.crawl(req, self)
