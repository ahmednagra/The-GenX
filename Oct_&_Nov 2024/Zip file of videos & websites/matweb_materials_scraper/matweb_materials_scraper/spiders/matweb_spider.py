from urllib.parse import urljoin
from collections import OrderedDict

from scrapy import Request, FormRequest

from .base import BaseSpider


class MatwebSpiderSpider(BaseSpider):
    name = "matweb_old"
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

    def start_requests(self):
        yield Request(url=self.start_urls[0], headers=self.headers, callback=self.parse)

    def parse(self, response, **kwargs):
        try:
            viewstat = response.css('#__VIEWSTATE ::attr("value")').get('')
            viewstat_generator_id = response.css('#__VIEWSTATEGENERATOR ::attr("value")').get('')
            categories = ['carbon', 'ceramic', 'metal', 'polymer']
            next_page = False
            for cat in categories[:1]:
                data = self.get_form_data(cat, viewstat, viewstat_generator_id, next_page)
                print('Category :', cat)
                yield FormRequest(url=self.start_urls[0], formdata=data, callback=self.parse_category_pagination,
                                  dont_filter=True, headers=self.headers, meta={'category':cat})
        except Exception as e:
            self.write_logs(f'Error In parse Function URL:{response.url}  \n Error:{e} \n\n')

    def parse_category_pagination(self, response):
        try:
            materials_urls = response.css('#tblResults a::attr(href)').getall()

            for mat_url in materials_urls[:1]:
                url = urljoin('https://www.matweb.com/', mat_url)
                yield Request(url,
                    callback=self.parse_detail,
                    headers=self.headers,
                    dont_filter=True,
                    meta=response.meta
                )

            next_page = response.css('a:contains("Next Page") ::attr(disabled)').get('')
            if not next_page:
                viewstat = response.css('#__VIEWSTATE ::attr("value")').get('')
                viewstat_generator_id = response.css('#__VIEWSTATEGENERATOR ::attr("value")').get('')
                cat = response.meta.get('category', '')
                next_page = True
                self.page_count += 1
                print('Page No Called', self.page_count)
                data = self.get_form_data(cat, viewstat, viewstat_generator_id, next_page)

                yield FormRequest(
                    url=self.start_urls[0],
                    formdata=data,
                    callback=self.parse_category_pagination,
                    dont_filter=True,
                    headers=self.headers,
                    meta={'category': cat, 'page_count': self.page_count}
                )

        except Exception as e:
            self.write_logs(f"Error in parse_category_pagination: {str(e)}")

    def parse_detail(self, response):
        try:
            item = OrderedDict()
            item['Name'] = response.css('.tabledataformat.t_ableborder th::text').get('').strip()
            item['Categories'] = ', '.join(response.css('th:contains("Categories:") + td a::text').getall()) or ', '.join(response.css('#ctl00_ContentMain_ucDataSheet1_trMatlGroups td a ::text').getall())
            item['Key Words'] = ', '.join(response.css('#ctl00_ContentMain_ucDataSheet1_trMatlNotes td ::text').getall())
            item['Vendors'] = self.get_vendor(response)
            item['Color'] = ''
            item['Crystal Structure'] = ''
            item['Physical Properties'] = self.get_properties(response, 'Physical' )
            item['Chemical Properties'] = self.get_properties(response, 'Chemical' )
            item['Mechanical Properties'] = self.get_properties(response, 'Mechanical' )
            item['Electrical Properties'] = self.get_properties(response, 'Electrical' )
            item['Thermal Properties'] = self.get_properties(response, 'Thermal' )
            item['Optical Properties'] = self.get_properties(response, 'Optical' )
            item['Component Elements Properties'] = self.get_properties(response, 'Component' )
            item['Descriptive Properties'] = self.get_properties(response, 'Descriptive' )
            item['Processing Properties'] = self.get_properties(response, 'Processing' )
            item['URL'] = response.url

            page_count = response.meta.get('page_count', '')
            item['Page NO'] = str(int(page_count) + 1) if page_count else '1'
            item['Category'] = response.meta.get('category', '')

            self.write_csv(item)

        except Exception as e:
            self.write_logs(f"Error in Item Yield URL:{response.url} & Error:{str(e)}")

    def get_form_data(self, category, viewstat, viewstat_generator_id, next_page):
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
        if next_page:
            next_page_data = {
                '__EVENTTARGET': 'ctl00$ContentMain$UcSearchResults1$lnkNextPage',
                'ctl00$ContentMain$UcSearchResults1$drpPageSize1': '200',
                'ctl00$ContentMain$UcSearchResults1$drpFolderList': '0',
                'ctl00$ContentMain$UcSearchResults1$txtFolderMatCount': '0/0',
                'ctl00$ContentMain$UcSearchResults1$drpPageSelect2': '1',
                'ctl00$ContentMain$UcSearchResults1$drpPageSize2': '50',
            }
            data.update(next_page_data)

            # Remove specific keys after updating with `next_page_data`
            data.pop('ctl00$ContentMain$btnSubmit.x', None)
            data.pop('ctl00$ContentMain$btnSubmit.y', None)

        return data

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