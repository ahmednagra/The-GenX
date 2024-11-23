import csv
import os
import glob
import json
from datetime import datetime
from collections import OrderedDict
from urllib.parse import urljoin, urlencode, quote

from scrapy import Request, Spider, Selector
from scrapy.exceptions import CloseSpider

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import TimeoutException, NoSuchElementException

import time
import re
from PIL import Image


class CurrySpider(Spider):
    name = "selenium_curry_products"
    allowed_domains = ["www.currys.co.uk"]
    base_url = 'https://www.currys.co.uk'
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        # 'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'CONCURRENT_REQUESTS': 3,

        # 'FEEDS': {
        #     f'output/zzCurrys Products Details {current_dt}.csv': {
        #     'format': 'csv',
        #         'fields': ['Keyword', 'SKU', 'Title', 'Brand', 'Price', 'Price Currency', 'Availability',
        #                    'Category', 'Description','Rich Content',  'Features', 'Specifications', 'Images', 'URL']
        #     }
        # }
    } # 'Rich Content' for js render description
    fields = ['Keyword', 'SKU', 'Title', 'Brand', 'Price', 'Price Currency', 'Availability',
                           'Category', 'Description', 'Features', 'Specifications', 'Images', 'URL']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/Currys_logs {self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

        # Initialize search keywords from the input file
        self.search_keywords = self.get_search_keywords_from_file()

        # Set up proxy key and usage flag
        self.proxy = self.get_scrapeops_api_key_from_file()

        self.items_scrapped = 0
        self.scraped_urls = []
        self.duplicates_count = 0

    def start_requests(self):
        # Close spider if proxy key is empty
        if not self.proxy:
            self.write_logs(f'ScrapeApi key not exist in input file. Scraper Closed')
            raise CloseSpider("Proxy key is missing. Closing spider.")

        # Log the total number of search keywords available
        self.write_logs(f'[START_REQUESTS] Number of search keywords: {len(self.search_keywords)}')

        for keyword in self.search_keywords:
            print(f'Search Keyword: {keyword}')
            url = f'https://www.currys.co.uk/search?q={quote(keyword)}&search-button=&lang=en_GB'
            yield Request(url, callback=self.parse, meta={'Keyword': keyword,
                                                          'handle_httpstatus_all': True, "zyte_api": {
                                                                                                                        "httpResponseBody": True,
                                                                                                                        "httpResponseHeaders": True,
                                                                                                                    }})

    def parse(self, response, **kwargs):
        keyword = response.meta.get('Keyword')

        # new url for get the 50 Products
        cgid = response.css('.page-next::attr("data-fullhref")').get('').split('&start=')[0]

        if cgid:
            url = f'https://www.currys.co.uk/search-update-grid?{cgid}&start=0&sz=50&viewtype=listView'
            yield Request(url, callback=self.pagination, dont_filter=True,
                              meta={'handle_httpstatus_all': True,
                                    'Keyword': keyword,"zyte_api": {
                                                                        "httpResponseBody": True,
                                                                        "httpResponseHeaders": True,
                                                                    }})
        else:
            self.write_logs(f'Keyword:{keyword} Not Found the search Update Url value, Keyword scraping skipped.')

    def pagination(self, response):
        keyword = response.meta.get('Keyword')
        try:
            items= response.css('.page-result-count::text, .search-result-count::text').get('').strip()
            self.write_logs(f'Keyword: {keyword} Total "{items}" are found\n')

            if not items:
                self.write_logs(f'Keyword: {keyword} NO product found\n')
                return

            # Extract unique product URLs
            products_urls = list(set(response.css('.click-beacon[role="columnheader"] > a::attr(href)').getall()))

            for product_url in products_urls[:2]:
                url = urljoin(self.base_url, product_url)

                #Avoiding Duplicate Records Scrape
                if url in self.scraped_urls:
                    self.duplicates_count += 1
                    self.write_logs(f'URL already scrapped, Skipped : {url} \n')
                    continue

                yield Request(url, callback=self.parse_product_detail,
                              meta={'Keyword': keyword, 'handle_httpstatus_all': True, "zyte_api": {
                                                                        "httpResponseBody": True,
                                                                        "httpResponseHeaders": True,
                                                                    }})

            # next page code is remiin

        except Exception as e:
            self.write_logs(f'[PARSE] Error on listing page for keyword "{keyword}": {e}')

    def parse_product_detail(self, response):
        keyword = response.meta.get('Keyword')
        title = ''
        # res = Selector(text=self.driver.page_source) if self.driver else response
        res = response
        url = response.url
        try:
            # Load product data from JSON-LD structured data script
            data_dict = json.loads(res.xpath('//script[@type="application/ld+json" and contains(text(), \'\"@type\":\"Product\"\')]/text()').get(''))

            price = data_dict.get('offers', {}).get('price', '')
            title = res.css('h1.product-name::text').get('').strip() or data_dict.get('name', '')

            # Create item dictionary with product details
            item = OrderedDict()
            item['Keyword'] = keyword
            item['Title'] = title
            item['Description'] = data_dict.get('description', '')
            item['SKU'] = data_dict.get('sku', '')
            item['Category'] = data_dict.get('category', '')
            item['Brand'] = data_dict.get('brand', {}).get('name', '')
            item['Price'] = price
            item['Availability'] ='In Stock' if price else 'Out of Stock'
            item['Price Currency'] = data_dict.get('offers', {}).get('priceCurrency', '')
            item['Images'] = '\n'.join([img.replace('?$l-large$&fmt=auto', '') for img in data_dict.get('image', [])])
            item['Features'] = '\n'.join([item.strip() for item in res.css('.key-features-desktop-tab .item-title ::text').getall()])
            item['Specifications'] = self.get_product_specifications(res, keyword, title)
            item['URL'] = response.url
            # item['Rich Content'] = str(self.get_rich_content())
            # try:
            #     item['Rich Content'] = str(self.get_rich_content())
            # except Exception as e:
            #     self.write_logs(f"[PARSE_PRODUCT_DETAIL] Error in 'get_rich_content': {e}")
            #     item['Rich Content'] = ''

            self.items_scrapped += 1
            print('Items ae Scrapped: ', self.items_scrapped)

            self.scraped_urls.append(url)
            # yield dict(item)
            self.write_csv(record=item)

        except json.JSONDecodeError as e:
            self.write_logs(f'[PARSE_PRODUCT_DETAIL] JSON parsing error for keyword "{keyword}" - {e}')
        except Exception as e:
            self.write_logs(f'[PARSE_PRODUCT_DETAIL] Error for keyword "{keyword}" and title "{title}": {e}')


    def get_scrapeops_api_key_from_file(self):
        key = self.get_input_from_txt(file_path=glob.glob('input/scraperapi_key.txt')[0])
        if not key:
            return ''

        api_endpoint = f'http://scraperapi:{key[0]}@proxy-server.scraperapi.com:8001'
        return api_endpoint

    def get_search_keywords_from_file(self):
        return self.get_input_from_txt(glob.glob('input/search_keywords.txt')[0])

    def get_input_from_txt(self, file_path):
        try:
            with open(file_path, mode='r') as txt_file:
                return [line.strip() for line in txt_file.readlines() if line.strip()]

        except FileNotFoundError:
            self.write_logs(f'[GET_INPUT_FROM_TXT] File not found: {file_path}')
            return []
        except Exception as e:
            self.write_logs(f'[GET_INPUT_FROM_TXT] Error reading file "{file_path}": {e}')
            return []

    def get_product_specifications(self, response, keyword, title):
        try:
            # Initialize spec to collect all specifications as a formatted string
            spec = ''

            # Extract and add the main title of the product specifications
            main_title = ''.join(response.css('#tab2 .productSheet > h3 ::text').getall()).strip()
            spec += main_title + '\n\n'

            # Select all specification sections
            specs_sections = response.css('.tech-specification-table')

            # Loop through each section to process its title and details
            for section in specs_sections:
                section_content  = ''

                # Extract and add the title of each specification section
                section_title = section.css('h3::text').get('')
                section_content  += section_title + '\n'

                # Process each row in the section to extract heading and value
                rows = section.css('.tech-specification-body')
                for row in rows:
                    heading = row.css('.tech-specification-th::text').get('')
                    value = ''.join([text.strip() for text in row.css('.tech-specification-td ::text').getall()])

                    # Format the row as 'Heading: Value'
                    section_content  += f'{heading}: {value}\n'

                # Add the section content to the main spec with a separator
                spec += section_content + '\n'

            return spec
        except Exception as e:
            self.write_logs(f'[GET_PRODUCT_SPECIFICATIONS] Error for keyword "{keyword}" and title "{title}": {e}')
            return ''

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    # def close(spider, reason):
    #     if spider.driver:
    #         spider.driver.quit()

    def get_scrapeops_url(self, url):
        payload = {'api_key': '1f4ba498-2bdb-42bc-8106-28c1c368b5c3', 'url': url}
        proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
        return proxy_url


    def get_rich_content(self):
        if 'View More' in self.driver.page_source or 'inpage-content' in self.driver.page_source:
            try:
                # Locate the "View More" button and click it
                view_more_button = WebDriverWait(self.driver, 20).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "btn.cta-primary-btn.view-more-content"))
                )

                WebDriverWait(self.driver, 20).until(
                    lambda d: d.execute_script('return document.readyState') == 'complete'
                )

                view_more_button.click()
                time.sleep(5)

                # Locate the rich media wrapper element by ID (flix-container)
                flix_container = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.ID, "flix-inpage"))
                )

                time.sleep(2)  # Short pause for initial content to stabilize

                screenshot_index = 1
                filename_base = re.sub(r'[^\w\-_\. ]', '_', self.driver.title)

                # Continuously scroll down and capture screenshots
                previous_height = self.driver.execute_script(
                    "return document.body.scrollHeight")  # Get initial page height

                time.sleep(3)

                while True:
                    # Capture and save screenshot of the flix-container
                    filename = f"{filename_base}_{screenshot_index}.png"
                    flix_container.screenshot(filename)
                    self.write_logs(f"Screenshot saved as {filename}")

                    # Increment screenshot index
                    screenshot_index += 1

                    # Scroll down the page by 500px
                    self.driver.execute_script("window.scrollBy(0, 500);")
                    time.sleep(2)
                    # time.sleep(3)  # Pause to allow more content to load
                    WebDriverWait(self.driver, 20).until(
                        lambda d: d.execute_script('return document.readyState') == 'complete'
                    )
                    # Check if the page height has increased, meaning more content has loaded
                    current_height = self.driver.execute_script("return document.body.scrollHeight")
                    if current_height == previous_height:
                        # If the page height hasn't changed, it means we've reached the end of the content
                        self.write_logs("No more content to load. Stopping screenshots.")
                        break

                    previous_height = current_height  # Update the previous height for next iteration

            except Exception as e:
                self.write_logs(f"Error occurred in get_rich_content: {e}")
            time.sleep(2)
            # get the text from view more section
            txt_content = []
            resp = Selector(text=self.driver.page_source)
            time.sleep(3)
            first_sect = resp.css('#flix-ek > *')
            for tag in first_sect:
                text = '\n'.join([txt.strip() for txt in tag.css('::text').getall() if txt.strip()])
                if text:
                    txt_content.append(text)

            sec_sect = resp.css('.flix-lang-en div[flixdata-type="template"]')
            for tag in sec_sect:
                text = '\n'.join([txt.strip() for txt in tag.css('::text').getall() if txt.strip()])
                if text:
                    txt_content.append(text)

            if not txt_content:
                tags = resp.css('#rich-media-wrapper > *')
                for tag in tags:
                    text = '\n'.join([txt.strip() for txt in tag.css('::text').getall() if txt.strip()])
                    if text:
                        txt_content.append(text)
            final_text = '\n\n'.join(txt_content)
            return final_text
        else:
            return ''

    def get_cookies(self, url):
        try:
            # Set up Chrome options for incognito mode
            chrome_options = Options()
            chrome_options.add_argument("--incognito")

            # Initialize the Chrome driver with options, using ChromeDriverManager
            self.driver = webdriver.Chrome(
                service=ChromeService(ChromeDriverManager().install()),
                options=chrome_options
            )

            # Open the specified URL
            self.driver.get(url)

            # Wait for the page to load completely
            WebDriverWait(self.driver, 10).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )

            # Check for the "Allow all" button and click if it exists
            try:
                allow_all_button = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'onetrust-accept-btn-handler'))
                )
                allow_all_button.click()
            except TimeoutException:
                # Log if the "Allow all" button is not found within the timeout
                self.write_logs(f'{url} Info: "Allow all" button not found or already accepted.')

            # Retrieve and return cookies as a dictionary
            cookies = {cookie['name']: cookie['value'] for cookie in self.driver.get_cookies()}
            return cookies

        except TimeoutException as e:
            self.write_logs(f'{url} Error: A TimeoutException occurred: {e}')
            self.write_logs(f'{url} Error: The page or one of the elements took too long to load.')
        except NoSuchElementException as e:
            self.write_logs(f'{url} Error: A NoSuchElementException occurred: {e}')
            self.write_logs(f'{url} Error: An element was not found on the page.')
        except Exception as e:
            self.write_logs(f'{url} Error: An unexpected error occurred: {e}')
        return None

    def write_csv(self, record):
        """Write a single record to the CSV file."""
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_file = f'{output_dir}/Currys Products Details {self.current_dt}.csv'

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

            print(f"Record for '{record.get('Title', '')}' written to CSV successfully.")
            a=1
        except Exception as e:
            print(f"Error writing to the CSV file: {e}")