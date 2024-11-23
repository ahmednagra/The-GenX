import json
import os
from copy import deepcopy
from datetime import datetime
from urllib.parse import urlencode
import scrapy
from markdownify import markdownify
from scrapy.crawler import CrawlerProcess
from scrapy.spidermiddlewares.httperror import HttpError
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from jobscraper.spiders import close_spider
from collections import defaultdict

MAX_JOBS = 1_000_000


class MorganstanleySpider(scrapy.Spider):
    name = "morganstanley"
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'content-type': 'application/json',
        'priority': 'u=1, i',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    }
    def params_list(self, start):
        params = [
            {
                'domain': 'morganstanley.com',
                'start': start,
                'num': '10',
                'exclude_pid': '549780152564',
                'pid': '549780152564',
                'BusinessArea': 'investment banking',
                'sort_by': 'relevance',
            },
            {
                'domain': 'morganstanley.com',
                'start': start,
                'num': '10',
                'exclude_pid': '549779960749',
                'pid': '549780414297',
                'BusinessArea': 'investment management',
                'sort_by': 'relevance',
            },
            {
                'domain': 'morganstanley.com',
                'start': start,
                'num': '10',
                'exclude_pid': '549779513672',
                'pid': '549779513672',
                'BusinessArea': 'research',
                'sort_by': 'relevance',
            },
            {
                'domain': 'morganstanley.com',
                'start': start,
                'num': '10',
                'exclude_pid': '549780024122',
                'pid': '549780024122',
                'BusinessArea': 'sales and trading',
                'sort_by': 'relevance',
            },
            {
                'domain': 'morganstanley.com',
                'start': start,
                'num': '10',
                'exclude_pid': '549779928918',
                'pid': '549779928918',
                'BusinessArea': 'technology',
                'sort_by': 'relevance',
            },
            {
                'domain': 'morganstanley.com',
                'start': start,
                'num': '10',
                'exclude_pid': '549780364924',
                'pid': '549780364924',
                'BusinessArea': 'global capital markets',
                'sort_by': 'relevance',
            }
        ]
        return params
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    scraped_jobs_dict = dict()
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    fetched_count = 0
    seen_jobs_count = 0
    run_completed = False

    def start_requests(self):
        self.max_jobs = getattr(self, "max_jobs", MAX_JOBS)
        self.max_jobs = int(self.max_jobs)
        self.seen_jobs_set = get_seen_jobs(self.name)
        base_url = 'https://morganstanley.eightfold.ai/api/apply/v2/jobs'
        start = 0
        params = self.params_list(start)
        for i, param in enumerate(params, start=0):
            url = f"{base_url}?{urlencode(param)}"
            job_count = 0

            try:
                yield scrapy.Request(
                    url,
                    callback=self.parse,
                    headers=self.headers,
                    meta={'job_count': job_count, 'start': start, 'iteration': i},
                    errback=self.handle_error
                )
            except json.JSONDecodeError as e:
                self.logger.error(f"⚠️ Invalid JSON: {e}")

    def handle_error(self, failure):
        self.logger.error(repr(failure))
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error("❌ HttpError on %s", response.url)
            self.logger.error("⛑️ Response body:\n%s", response.body)
            self.logger.error("🔎 Request headers:\n%s", response.request.headers)

    def parse(self, response, **kwargs):
        iteration = response.meta.get('iteration')
        start = response.meta.get('start')
        job_count = response.meta.get('job_count')
        data = json.loads(response.text)
        if data:
            total_jobs = data.get('count')
            self.logger.info(f"🎃 Total jobs: {total_jobs} | Job count: {job_count} | Start: {start} | Iteration: {iteration}")
            positions = data.get('positions')
            # self.remove_obsolete_jobs = self.max_jobs >= total_jobs

            if positions:
                for job in positions:
                    job_count = job_count + 1
                    id = job.get('id')
                    if self.fetched_count >= self.max_jobs:
                        self.remove_obsolete_jobs = False
                        self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                        return

                    job_dict = {"title": job.get("name"), "id": job.get("display_job_id")}
                    id_unique = get_id_unique(self.name, job_dict)
                    job_dict = defaultdict(lambda: None, job_dict)
                    self.scraped_jobs_dict[id_unique] = job_dict["title"]
                    self.seen_jobs_count += 1
                    if id_unique in self.seen_jobs_set:
                        self.logger.info(
                            f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                        )
                        continue
                    self.fetched_count += 1
                    job_dict["id_unique"] = id_unique
                    job_url = f"https://morganstanley.eightfold.ai/api/apply/v2/jobs/{id}?domain=morganstanley.com"
                    yield scrapy.Request(job_url, callback=self.detail, meta={"item": job_dict})
            if job_count < total_jobs:
                start += 10
                param = self.params_list(start)[iteration]
                base_url = 'https://morganstanley.eightfold.ai/api/apply/v2/jobs'
                url = f"{base_url}?{urlencode(param)}"
                yield scrapy.Request(
                    url,
                    callback=self.parse,
                    headers=self.headers,
                    meta={'job_count': job_count, 'start': start, 'iteration': iteration}, errback=self.handle_error
                )

    def detail(self, response):
        item = response.meta.get("item")
        data = json.loads(response.body)
        data_fields = data.get("custom_JD", {}).get("data_fields", {})
        further_info = {
            "id_unique": item["id_unique"],
            "title": data.get('name'),
            "url": data.get("canonicalPositionUrl", ""),
            "description": markdownify(data.get("job_description", "")),
            "location": [i.split(",", 1)[0].strip() for i in data.get("locations", [])],
            "industry": data.get("department", ""),
            "business_unit": data.get("business_unit", ""),
            "type": data.get("type", ""),
            "apply_link": data.get("apply_redirect_url", ""),
            "posted_date": data_fields.get("posteddate", [""])[0],
            "job_type": data_fields.get("employmenttype", ""),
            "job_level": data_fields.get("joblevel", ""),
        }
        job_posting_text = f"""Job title:\n {further_info['title']}
        Description:\n {further_info['description']}
        """
        job_info = get_job_info(job_posting_text)
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MorganstanleySpider)
    process.start()
