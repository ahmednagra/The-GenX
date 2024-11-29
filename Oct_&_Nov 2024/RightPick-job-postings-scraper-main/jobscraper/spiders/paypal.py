import os
import re
import json
from math import ceil
from typing import Any
from datetime import datetime

# import Functions
from jobscraper.spiders import close_spider
from dataextraction import get_job_info, get_seen_jobs, get_id_unique

# Import third Parties Packages
from markdownify import markdownify

import scrapy
from scrapy.http import Response
from scrapy.crawler import CrawlerProcess
from scrapy.spidermiddlewares.httperror import HttpError


MAX_JOBS = 1_000_000

class PaypalSpider(scrapy.Spider):
    name = "paypal"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False

    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,

        "CONCURRENT_REQUESTS": 4
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://paypal.eightfold.ai/careers?pid=274902547165&Job%20Category=Software%20Development&Job%20Category=Machine%20Learning&Job%20Category=Data%20Science&Job%20Category=Inside%20Sales&Job%20Category=Engineering%20Program%20Management&Job%20Category=Enterprise%20Sales&Job%20Category=Customer%20Success%20Management&Job%20Category=Business%20Development&Job%20Category=Marketing&Job%20Category=Data%20Engineering&Job%20Category=Product%20Sales&Job%20Category=Corporate%20Strategy&Job%20Category=Business%20Program%20Management&Job%20Category=Customer%20Solutions&Job%20Category=Database%20Marketing&Job%20Category=Technical%20Architect&Job%20Category=Business%20Intelligence&Job%20Category=Executive%20Management&Job%20Category=Partnership%20Marketing&Job%20Category=Sales%20Operations&Job%20Category=Business%20Process&Job%20Category=Business%20Project%20Management&Job%20Category=Corporate%20Dev%20%26%20Ventures&Job%20Category=Database%20Engineering&Job%20Category=Market%20Research&Job%20Category=Marketing%20Planning%20Operations&Job%20Category=Performance%20Marketing&Job%20Category=Program%20Management&Job%20Category=Quantitative%20Analytics&Job%20Category=Sales%20Development&Job%20Category=Solutions%20Engineering&Job%20Category=Network%20Engineering&Job%20Category=Technical%20Product%20Management&Job%20Category=Business%20Operations&domain=paypal.com&sort_by=relevance&triggerGoButton=false',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }

    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    run_completed = False


    # test
    total_jobs_found = 0
    total_pages = 0
    total_jobs_scraped = 0
    total_jobs_skipped = 0

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        self.remove_obsolete_jobs = self.max_jobs >= MAX_JOBS

        self.logger.info('\n\n\n')
        self.logger.info(f'Previously Jobs Found in the cache are: {len(self.seen_jobs_set)}')
        self.logger.info(f'Paypal Jobs Spider limit is: {self.max_jobs}')
        self.logger.info(f'Paypal Jobs Spider self.remove_obsolete_jobs: {self.remove_obsolete_jobs} \n\n\n')

        start = 1
        get_url = f'https://paypal.eightfold.ai/api/apply/v2/jobs?domain=paypal.com&start={start}&num=10&exclude_pid=274902547165&pid=274902547165&Job Category=Software Development&Job Category=Machine Learning&Job Category=Data Science&Job Category=Inside Sales&Job Category=Engineering Program Management&Job Category=Enterprise Sales&Job Category=Customer Success Management&Job Category=Business Development&Job Category=Marketing&Job Category=Data Engineering&Job Category=Product Sales&Job Category=Corporate Strategy&Job Category=Business Program Management&Job Category=Customer Solutions&Job Category=Database Marketing&Job Category=Technical Architect&Job Category=Business Intelligence&Job Category=Executive Management&Job Category=Partnership Marketing&Job Category=Sales Operations&Job Category=Business Process&Job Category=Business Project Management&Job Category=Corporate Dev & Ventures&Job Category=Database Engineering&Job Category=Market Research&Job Category=Marketing Planning Operations&Job Category=Performance Marketing&Job Category=Program Management&Job Category=Quantitative Analytics&Job Category=Sales Development&Job Category=Solutions Engineering&Job Category=Network Engineering&Job Category=Technical Product Management&Job Category=Business Operations&domain=paypal.com&sort_by=relevance'
        yield scrapy.Request(get_url, callback=self.pagination, headers=self.headers, meta={'sops_keep_headers': True}, errback=self.handle_error)

    def handle_error(self, failure):
        # log all failures
        self.logger.error(repr(failure))
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error("❌ HttpError on %s", response.url)
            self.logger.error("⛑️ Response body:\n%s", response.body)
            self.logger.error("🔎 Request headers:\n%s", response.request.headers)
            # self.logger.error('🔎 Request body:\n%s', response.request.body)

    def pagination(self, response):

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as e:
            self.logger.info('\n\n\n')
            self.logger.info(f'Unable to load script into Dict. Error : {e} URL: {response.url}\n\n\n')
            data= {}
            return

        self.total_jobs_found = data.get('count')

        total_pages = ceil(self.total_jobs_found / 10)
        for page_no in range(0, total_pages + 1):

            # test
            self.total_pages += 1

            start = int(page_no) * 10 + 1
            get_url = f'https://paypal.eightfold.ai/api/apply/v2/jobs?domain=paypal.com&start={start}&num=10&exclude_pid=274902547165&pid=274902547165&Job Category=Software Development&Job Category=Machine Learning&Job Category=Data Science&Job Category=Inside Sales&Job Category=Engineering Program Management&Job Category=Enterprise Sales&Job Category=Customer Success Management&Job Category=Business Development&Job Category=Marketing&Job Category=Data Engineering&Job Category=Product Sales&Job Category=Corporate Strategy&Job Category=Business Program Management&Job Category=Customer Solutions&Job Category=Database Marketing&Job Category=Technical Architect&Job Category=Business Intelligence&Job Category=Executive Management&Job Category=Partnership Marketing&Job Category=Sales Operations&Job Category=Business Process&Job Category=Business Project Management&Job Category=Corporate Dev & Ventures&Job Category=Database Engineering&Job Category=Market Research&Job Category=Marketing Planning Operations&Job Category=Performance Marketing&Job Category=Program Management&Job Category=Quantitative Analytics&Job Category=Sales Development&Job Category=Solutions Engineering&Job Category=Network Engineering&Job Category=Technical Product Management&Job Category=Business Operations&domain=paypal.com&sort_by=relevance'

            yield scrapy.Request(get_url, callback=self.parse, headers=self.headers,
                                 meta={'sops_keep_headers': True}, dont_filter=True,
                                 errback=self.handle_error)

    def parse(self, response: Response, **kwargs: Any) -> Any:
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as e:
            self.logger.info('\n\n\n')
            self.logger.info(f'Unable to load script into Dict. Error : {e} URL: {response.url}\n\n\n')
            return

        positions = data.get('positions', [])
        for position in positions:
            id = position.get('id', '')

            try:
                if self.fetched_count >= self.max_jobs:
                    self.logger.info('\n\n\n')
                    self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...\n\n\n")
                    return

                title = position.get('name', '')
                location = position.get('locations', '')
                location = ["Edinburgh" if "Scotland" in loc else loc for loc in location]

                department = position.get('department', '')
                work_location_option = position.get('work_location_option', '')
                business_unit = position.get('business_unit', '')
                job_url = position.get('canonicalPositionUrl', '')
                job_dict = {
                    'id': str(id),
                    'url': job_url,
                    'title': title,

                }

                id_unique = get_id_unique(
                    self.name, job_dict, id=job_dict["id"], title=job_dict["title"]
                )

                job_dict["id_unique"] = id_unique
                self.scraped_jobs_dict[id_unique] = job_dict["title"]
                self.seen_jobs_count += 1

                if id_unique in self.seen_jobs_set:
                    self.logger.info('\n\n\n')
                    self.logger.info(f'👀 Job "{job_dict["title"]}" already seen. Skipping...\n\n\n')
                    self.total_jobs_skipped += 1
                    continue

                self.fetched_count += 1

                headers = {
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
                    'sec-ch-ua': '"Brave";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                }
                yield scrapy.Request(job_url, callback=self.detail_page, headers=headers,
                                     meta={'sops_keep_headers': True, 'id': id, 'title': title,
                                           'location': location, 'department': department,
                                           'work_location_option': work_location_option,
                                           'business_unit': business_unit})

            except Exception as e:
                self.logger.info('\n\n\n')
                self.logger.info(f'Error {e} In make new request where job id :{id} and url {response.url} \n\n\n')


    def detail_page(self, response):
        id = str(response.meta.get('id'))
        title = response.meta.get('title')
        location = response.meta.get('location')
        department = response.meta.get('department')
        work_location_option = response.meta.get('work_location_option')
        business_unit = response.meta.get('business_unit')

        try:
            # Extract JSON data from the response
            data1 = response.css('[type="application/ld+json"]::text').get('')
        except Exception as e:
            self.logger.info('\n\n\n')
            self.logger.info(f'Paypal Job ID {id} Error Found in Script Tag to load:{e}  Url :{response.url}\n\n\n')
            return

        # Clean the JSON string to remove invalid control characters
        if data1:
            try:
                # Remove control characters such as newlines, tabs, etc.
                data1_clean = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', data1)

                try:
                    data = json.loads(data1_clean)
                except json.JSONDecodeError as e:
                    self.logger.info('\n\n\n')
                    self.logger.info(f'Unable to load script into Dict. Error : {e} URL: {response.url}\n\n\n')
                    self.total_jobs_skipped += 1
                    return

                description = data.get('description', '')
                description = markdownify(description)
                datePosted = data.get('datePosted', '')
                validThrough = data.get('validThrough', '')
                employmentType = data.get('employmentType', '')
                hiringOrganization = data.get('hiringOrganization', {}).get('name', '')
                url = data.get('url', '')
                apply_url = url + '#apply'

                further_info = {
                    'id': id,
                    'title': title,
                    'location': location,
                    'department': department,
                    'work_location_option': work_location_option,
                    'business_unit': business_unit,
                    'description': description,
                    'date_posted': datePosted,
                    'valid_through': validThrough,
                    'employment_type': employmentType,
                    'hiring_organization': hiringOrganization,
                    'url': url,
                    'apply_url': apply_url
                }
                job_posting_text = f"""Job title:\n {further_info['title']}
                                                        Description:\n{further_info['description']}
                                                        """
                job_info = get_job_info(job_posting_text)

                if job_info.get('error', {}):
                    self.logger.info('\n\n\n')
                    self.logger.info(f"Detail Page Url : {response.url}  Job Id = {id}, Job Title :{title} Not Successfully and get an error From CHatGpt Model :{job_info.get('error', {})}  ")
                    self.total_jobs_skipped += 1
                    return

                else:
                    self.total_jobs_scraped += 1

                    print(f'Total Jobs are Scrapped: {self.total_jobs_scraped}')
                    yield {**further_info, **job_info}

            except Exception as e:
                self.logger.info('\n\n\n')
                self.logger.info(f'error Yield item {response.url}  Error: {e}\n\n\n')


    def closed(spider, reason):
        spider.logger.info('\n\n\n\n\n\n')
        spider.logger.info(f'Total Pages Called :{spider.total_pages}')
        spider.logger.info(f'Total Jobs Found :{spider.total_jobs_found}')
        spider.logger.info(f'Total Jobs Scraped  :{spider.total_jobs_scraped}')
        spider.logger.info(f'Total Jobs Skipped  :{spider.total_jobs_skipped}\n\n\n\n\n\n')

        close_spider(spider, reason)

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(PaypalSpider)
    process.start()



