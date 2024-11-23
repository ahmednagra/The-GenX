import json
import os
from math import ceil
from shutil import which
from time import sleep
from datetime import datetime
from typing import OrderedDict

import requests

from jobscraper.spiders import close_spider
# from dataextraction import get_job_info, get_seen_jobs, get_id_unique

from markdownify import markdownify

from scrapy.crawler import CrawlerProcess
from scrapy import Spider, Request, Selector
import requests
from urllib.parse import urlencode

# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.chrome.service import Service as ChromeService

# from scrapy_playwright.page import PageMethod

from scrapy_selenium import SeleniumRequest
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver import ChromeOptions


MAX_JOBS = 1_000_000
get_proxy_url = lambda url: url


class TiktokSpider(Spider):
    name = "tiktok"
    main_url = 'https://careers.tiktok.com/'

    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        # 'SCRAPEOPS_PROXY_SETTINGS': {'render_js': True},
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],

        # Include DOWNLOADER_MIDDLEWARES in custom settings
        # 'DOWNLOADER_MIDDLEWARES': {
        #     'scrapy_selenium.SeleniumMiddleware': 800
        # }
    }
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    run_completed = False

    def __init__(self):
        super().__init__()
        self.items_scraped = 0

        # Selenium Driver
        # self.homepage_url = None
        # self.driver = None

        # self.SELENIUM_DRIVER_NAME = 'chrome'
        # self.SELENIUM_DRIVER_EXECUTABLE_PATH = which('chromedriver')
        # self.SELENIUM_DRIVER_ARGUMENTS = ['--headless']

        self.cookies = {}
        self.headers = {}
        self.page_no = 1
        self.base_url = (
            'https://careers.tiktok.com/position?keywords=&category=6704215901438216462%2C6709824272505768200%2C6704215864629004552%2C6704215882479962371%2C6704215862603155720&location=&project=&type=&job_hot_flag=&current={}&limit=250&functionCategory=&tag='
        )

        # self.page_methods = [
        #     PageMethod('evaluate', '''() => {
        #             return document.body.innerText.includes('Find Your New Job');
        #         }'''),
        #     PageMethod('wait_for_timeout', 2000),
        #     PageMethod('screenshot', path="screenshot.png", full_page=True),
        # ]

    def start_requests(self):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            # 'Cookie': '_ttp=2l1DmrhlIInGISh4mqwI1wtQFX3; tt_chain_token=nVUeMur71iNbmIY7KP8vgw==; ttwid=1%7CVs4NpfXmkCrxDFNCQyZUDXdoDdficnm2XB3wFOb-bQI%7C1727861219%7Cd3bed10f68157ae8d057adb1b55469434b17d58570e96579da31c6ae1d7c5d98; locale=en-US; s_v_web_id=verify_m2dome93_41dAHBsd_xO0d_4ufs_Ak5u_ZmYKoKuPREBA; atsx-csrf-token=7ze5GmM2Obxtrti7b8D-oimUyujVlqzUaOC77xm7aMA%3D',
            'Origin': 'https://careers.tiktok.com',
            'Portal-Channel': 'tiktok',
            'Portal-Platform': 'pc',
            'Referer': 'https://careers.tiktok.com/position?keywords=&category=6704215901438216462%2C6709824272505768200%2C6704215864629004552%2C6704215882479962371%2C6704215862603155720&location=&project=&type=&job_hot_flag=&current=1&limit=250&functionCategory=&tag=',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'accept-language': 'en-US',
            'env': 'undefined',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'website-path': 'tiktok',
            # 'x-csrf-token': '7ze5GmM2Obxtrti7b8D-oimUyujVlqzUaOC77xm7aMA=',
            'atsx-csrf-token': 'bPQnVymnC9ou7tgTogOTk8mknXLfDW8fmtv50HWnQUU%3D',   # new token
        }
        params = {
            'keyword': '',
            'limit': '250',
            'offset': '0',
            'job_category_id_list': '6704215901438216462,6709824272505768200,6704215864629004552,6704215882479962371,6704215862603155720',
            'tag_id_list': '',
            'location_code_list': '',
            'subject_id_list': '',
            'recruitment_id_list': '',
            'portal_type': '6',
            'job_function_id_list': '',
            'storefront_id_list': '',
            'portal_entrance': '1',
            # '_signature': 'C86JgQAAAAApQDWeCi9-7QvOiZAAGzm',
        }
        json_data = {
            'keyword': '',
            'limit': 250,
            'offset': 0,
            'job_category_id_list': [
                '6704215901438216462',
                '6709824272505768200',
                '6704215864629004552',
                '6704215882479962371',
                '6704215862603155720',
            ],
            'tag_id_list': [],
            'location_code_list': [],
            'subject_id_list': [],
            'recruitment_id_list': [],
            'portal_type': 6,
            'job_function_id_list': [],
            'storefront_id_list': [],
            'portal_entrance': 1,
        }
        cookies = {
            # '_ttp': '2l1DmrhlIInGISh4mqwI1wtQFX3',
            # 'tt_chain_token': 'nVUeMur71iNbmIY7KP8vgw==',
            # 'ttwid': '1%7CVs4NpfXmkCrxDFNCQyZUDXdoDdficnm2XB3wFOb-bQI%7C1727861219%7Cd3bed10f68157ae8d057adb1b55469434b17d58570e96579da31c6ae1d7c5d98',
            'locale': 'en-US',
            's_v_web_id': 'verify_m2g5ohwp_017ToPXf_vQwq_4ueM_BQKV_S2RHKSHvunNa',
            'atsx-csrf-token': 'bPQnVymnC9ou7tgTogOTk8mknXLfDW8fmtv50HWnQUU%3D',
        }

        url = 'https://careers.tiktok.com/api/v1/search/job/posts?keyword=&limit=250&offset=0&job_category_id_list=6704215901438216462,6709824272505768200,6704215864629004552,6704215882479962371,6704215862603155720&tag_id_list=&location_code_list=&subject_id_list=&recruitment_id_list=&portal_type=6&job_function_id_list=&storefront_id_list=&portal_entrance=1'

        api_post_request = requests.post(url=self.get_scrapeops_url(url),
        # api_post_request = requests.post(url=url,
            params=params,
            headers=headers,
            json=json_data,
                                         # cookies=cookies
        )
        cookies = api_post_request.cookies
        cookies_dict = requests.utils.dict_from_cookiejar(cookies)
        print(cookies_dict)
        a=1

    def get_scrapeops_url(self, url):
        payload = {'api_key': 'e21c645a-361a-4874-8dcf-816e85a40c77', 'url': url, 'render_js': True}
        proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
        return proxy_url

    def make_post_request_with_selenium(self, response):
        driver = response.meta['driver']

        # JavaScript code to perform the POST request
        post_script = """
        return fetch('https://careers.tiktok.com/api/v1/search/job/posts', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                // Include other headers if necessary
            },
            body: JSON.stringify(arguments[0])
        }).then(response => response.json()).then(data => data);
        """

        # Execute the script using Selenium
        post_response = driver.execute_script(post_script, response.meta['json_data'])

        # Process the data received from the POST request
        self.parse_indexing_page(post_response)

        # yield Request(
        #     url=proxy_url,
        #     callback=self.parse_indexing_page,
        #     meta=dict(
        #         playwright=True,
        #         playwright_include_page=True,
        #         playwright_page_methods=self.page_methods,
        #         errback=self.errback,
        #     ))

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()

    def parse_indexing_page(self, response):
        print('parse Method')
        return
        page = response.meta['playwright_page']
        # cookies = await page.context.cookies()
        self.cookies = {cookie['name']: cookie['value'] for cookie in cookies}


        res = Selector(text=self.driver.page_source)
        job_ids = res.css('.rightBlock a::attr(data-id) ').getall() or []

        total_jobs = res.css('.rightBlock > div > div::text').re_first(r'\d+')
        if not total_jobs:
            total_jobs = res.css('.atsx-pagination.mini li::attr(title)').getall()[-2]

        if not job_ids:
            self.driver.refresh()
            sleep(15)

            # Wait until 'Find Your New Job' is in the page source
            WebDriverWait(self.driver, 20).until(
                lambda driver: 'Find Your New Job' in driver.page_source
            )

            if 'Find Your New Job' not in self.driver.page_source:
                sleep(15)

            res = Selector(text=self.driver.page_source)
            job_ids = res.css('.rightBlock a::attr(data-id) ').getall() or []

        # Iterate over job IDs and yield requests for job details
        for job_id in job_ids:
            job_url = f'https://careers.tiktok.com/api/v1/job/posts/{job_id}?portal_type=6&with_recommend=false&portal_type=6&with_recommend=false'
            self.headers['Referer'] = f'https://careers.tiktok.com/position/{job_id}/detail'

            yield Request(url=job_url, headers=self.headers, cookies=self.cookies, callback=self.parse_detail_page,
                          meta={'handle_httpstatus_all': True, "sops_keep_headers": True})

        # Handle pagination
        pages = ceil(int(total_jobs) / 250)
        while self.page_no <= pages:
            self.page_no += 1
            url = self.base_url.format(self.page_no)
            self.driver.get(url)

            sleep(10)

            # Wait until 'Find Your New Job' is in the page source
            WebDriverWait(self.driver, 20).until(
                lambda driver: 'Find Your New Job' in driver.page_source
            )

            if 'Find Your New Job' not in self.driver.page_source:
                sleep(15)

            # Get job IDs from the current page
            html = Selector(text=self.driver.page_source)
            job_ids = html.css('.rightBlock a::attr(data-id) ').getall() or []

            for job_id in job_ids:
                job_url = f'https://careers.tiktok.com/api/v1/job/posts/{job_id}?portal_type=6&with_recommend=false'
                self.headers['Referer'] = f'https://careers.tiktok.com/position/{job_id}/detail'
                yield Request(url=job_url, headers=self.headers, cookies=self.cookies,
                              callback=self.parse_detail_page, meta={'handle_httpstatus_all': True,
                                                                     "sops_keep_headers": True})

    def parse_detail_page(self, response):
        try:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(
                    f"🛑 Reached max jobs: {self.max_jobs}. Stopping..."
                )
                return
            data = response.json()
            job_dict = data.get('data', {}).get('job_post_detail', {})

            id = job_dict.get('id', '')
            title = job_dict.get('title', '')
            description = job_dict.get('description', '')
            requirements = job_dict.get('requirement', '')
            category = job_dict.get('job_category', {}).get('en_name', '')
            location = [city.get('en_name') for city in job_dict.get('city_list', [{}])]
            job_url = f'https://careers.tiktok.com/position/{id}/detail'
            job_dict = {
                "id": str(id),
                "url": job_url,
                "title": title,
            }
            id_unique = get_id_unique(
                self.name, job_dict, id=str(job_dict["id"]), title=job_dict["title"]
            )
            job_dict["id_unique"] = id_unique
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in self.seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                )
                return
            self.fetched_count += 1

            job_info = data.get('data', {}).get('job_post_detail', {}).get('job_post_info', {}).get(
                'job_post_object_value_map', '')

            job_detail = ''

            if job_info:
                for value in job_info.values():
                    if value:
                        job_detail = value
                        job_detail = markdownify(job_detail)

            description = description + '\n' + requirements + '\n' + job_detail
            further_info = {
                'id': id,
                'title': title,
                'description': description,
                'category': category,
                'location': location,
                'url': job_url,
            }
            job_posting_text = f"""Job title:\n {further_info['title']}
                                                    Description:\n{further_info['description']}
                                                    """
            job_info = get_job_info(job_posting_text)
            yield {**further_info, **job_info}


        except Exception as e:
            self.logger.warning(f"⚠️ Received Error:{e} error from {response.url}. Skipping this Job Details URL.")

    def get_cookies_headers(self):
        try:
            # Setup Chrome options for incognito mode
            # chrome_options = Options()
            # chrome_options.add_argument("--incognito")
            #
            # # Automatically install the ChromeDriver using webdriver_manager
            # self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()),
            #                                options=chrome_options)
            #
            # # Open the website
            # url = self.base_url.format(self.page_no)
            # self.driver.get(url)
            #
            # WebDriverWait(self.driver, 20).until(
            #     lambda driver: 'Find Your New Job' in driver.page_source
            # )

            # Sleep to ensure all elements are fully rendered (adjust as needed)
            sleep(10)

            driver_cookies = self.driver.get_cookies()
            csrf_token = ''.join(
                [cookie.get('value', '') for cookie in driver_cookies if cookie.get('name', '') == 'atsx-csrf-token'])
            cookies = {
                'locale': 'en-US',
                's_v_web_id': ''.join(
                    [cookie.get('value', '') for cookie in driver_cookies if cookie.get('name', '') == 's_v_web_id']),
                'atsx-csrf-token': csrf_token,
            }

            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'Origin': 'https://careers.tiktok.com',
                'Portal-Channel': 'tiktok',
                'Portal-Platform': 'pc',
                'Referer': 'https://careers.tiktok.com/position?keywords=&category=6704215901438216462%2C6709824272505768200%2C6704215864629004552%2C6704215882479962371%2C6704215862603155720&location=&project=&type=&job_hot_flag=&current=1&limit=10&functionCategory=&tag=',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
                'accept-language': 'en-US',
                'env': 'undefined',
                'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'website-path': 'tiktok',
                'x-csrf-token': csrf_token,
            }

            if 'Find Your New Job' not in self.driver.page_source:
                sleep(15)

            return cookies, headers
        except Exception as e:
            self.logger.error(f"⚠️Error in the Selenium Driver at First Page while error:{e}")

    def closed(self, reason):
        a=1
        # close the opened Selenium Chrome browser
        # if self.driver:
        #     self.driver.quit()
        #     close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(TiktokSpider)
    process.start()
