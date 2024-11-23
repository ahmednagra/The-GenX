import json
import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from collections import defaultdict
import os
from jobscraper.spiders import close_spider

MAX_JOBS = 1_000_000


class BarclaysSpider(scrapy.Spider):
    name = "barclays"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }

    headers = {
        'accept': 'application/json',
        'accept-language': 'en-US',
        'content-type': 'application/json',
        'origin': 'https://blackstone.wd1.myworkdayjobs.com',
        'referer': 'https://blackstone.wd1.myworkdayjobs.com/en-US/Blackstone_Careers',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'x-calypso-csrf-token': 'b7423b8a-e5a3-4c9c-ab3f-91d5042c93a2',
    }

    urls_and_facets = {
        "https://blackstone.wd1.myworkdayjobs.com/wday/cxs/blackstone/Blackstone_Careers/jobs": [
            '7674c604ae9701fa615f1fec7a11d2a3',
            '7674c604ae9701fc079e63027b11fca3',
            '7674c604ae970156f286e2227b1119a4',
            '6afbaffc245201f8ed466dc91701a96d',
            '0620793d7007010101bdc3532a650000',
            '936671cbf22310014b577fbf14dc0000',
            '14a4c2700875100152ee74ab40220000',
            '7674c604ae97016f03c59f207b1111a4',
            '7674c604ae97017b7fa8d1277b111ba4',
            '7674c604ae9701d8efaef7077b1108a4'
        ],
        "https://blackstone.wd1.myworkdayjobs.com/wday/cxs/blackstone/Blackstone_Campus_Careers/jobs": [
            '7674c604ae9701b4305c711b7b110fa4',
            '7674c604ae970156f286e2227b1119a4',
            '7674c604ae9701709fd117147b110ba4',
            '7674c604ae9701c843715f2b7b111da4',
            '7674c604ae9701d8efaef7077b1108a4',
            '7674c604ae9701fc079e63027b11fca3',
        ]
    }

    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    remove_obsolete_jobs = False
    total_jobs = None
    run_completed = False

    def pay_load(self, url, offset, limit):
        facets = self.urls_and_facets.get(url, [])  # Get the correct facets based on the URL
        json_data = {
            'appliedFacets': {
                'REC_ESI_Skill_is_Business_Unit_Extended': facets
            },
            'limit': limit,
            'offset': offset,
            'searchText': '',
        }
        return json.dumps(json_data)

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS))
        self.remove_obsolete_jobs = self.max_jobs >= MAX_JOBS

        self.urls_to_process = list(self.urls_and_facets.keys())  # List of URLs to process sequentially
        self.current_url_index = 0  # Start from the first URL

        # Start with the first URL
        if self.urls_to_process:
            current_url = self.urls_to_process[self.current_url_index]
            offset = 0
            limit = 20
            yield scrapy.Request(
                url=current_url,
                method='POST',
                headers=self.headers,
                body=self.pay_load(current_url, offset, limit),
                callback=self.parse,
                meta={'offset': offset, 'limit': limit, 'is_initial': True, 'url': current_url, 'sops_keep_headers': True}
            )

    def parse(self, response):
        data = json.loads(response.body)
        jobs = data.get('jobPostings', [])
        if response.meta.get('is_initial'):
            self.total_jobs = data.get('total', 0)
            self.logger.info(f'Total jobs available: {self.total_jobs}')

        offset = response.meta['offset']
        limit = response.meta['limit']
        current_url = response.meta['url']

        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            base_url = "https://blackstone.wd1.myworkdayjobs.com/en-US/Blackstone_Careers"
            url = base_url + job.get("externalPath", "")
            job_dict = {
                "url": url,
                "title": job.get("title"),
                "id": str(job.get("bulletFields", "")[0])
            }

            id_unique = get_id_unique(self.name, job_dict, id=str(job_dict["id"]), title=job_dict["title"])
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in self.seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1

            yield scrapy.Request(
                url=url,
                callback=self.job_detail,
                meta={"job_dict": job_dict},
            )

        # Pagination logic: if more jobs are available for the current URL
        if self.total_jobs and offset + limit < self.total_jobs:
            offset += limit
            self.logger.info(f"Fetching more jobs for {current_url} with offset {offset}")
            yield scrapy.Request(
                url=current_url,
                method='POST',
                headers=self.headers,
                body=self.pay_load(current_url, offset, limit),
                callback=self.parse,
                meta={'offset': offset, 'limit': limit, 'url': current_url, 'sops_keep_headers': True}
            )
        else:
            # Move to the next URL after finishing the current one
            self.current_url_index += 1
            if self.current_url_index < len(self.urls_to_process):
                next_url = self.urls_to_process[self.current_url_index]
                self.logger.info(f"Switching to next URL: {next_url}")
                offset = 0
                yield scrapy.Request(
                    url=next_url,
                    method='POST',
                    headers=self.headers,
                    body=self.pay_load(next_url, offset, 20),
                    callback=self.parse,
                    meta={'offset': offset, 'limit': 20, 'is_initial': True, 'url': next_url, 'sops_keep_headers': True}
                )

    def job_detail(self, response):
        script_data = response.css('script[type="application/ld+json"]::text').get()
        data = json.loads(script_data)
        further_info = response.meta.get('job_dict')
        location = data.get('jobLocation', {}).get('address', {}).get('addressLocality', '')
        if location == "New York 601 Lex":
            location = "New York City"
        elif location == "Cambridge":
            location = "Cambridge, MA"
        elif location == "Berkeley Square House London":
            location = "London"
        elif location == "New Jersey":
            location = "Princeton, NJ"
        further_info["location"] = [location]
        further_info["posted_date"] = data.get('datePosted', '')

        further_info["employment_type"] = data.get('employmentType', '')
        further_info["description"] = markdownify(data.get('description', ''))
        job_posting_text = f"""Job title:\n {further_info['title']}
                                Description:\n{further_info['description']}
                                """
        job_info = get_job_info(job_posting_text)
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(BarclaysSpider)
    process.start()
