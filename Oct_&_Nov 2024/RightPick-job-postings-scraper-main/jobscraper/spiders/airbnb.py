import html
import json
from copy import deepcopy
import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from collections import defaultdict
import os
from jobscraper.spiders import close_spider

MAX_JOBS = 1_000_000


class AirbnbSpider(scrapy.Spider):
    name = "airbnb"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "dnt": "1",
        "origin": "https://careers.airbnb.com",
        "priority": "u=1, i",
        "referer": "https://careers.airbnb.com/positions/?_paged=3",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    json_data = {
        "action": "facetwp_refresh",
        "data": {
            "facets": {
                "jobs_total": [],
                "search_input": "",
                "departments": [],
                "offices": [],
                "workplace_type": [],
                "jobs_pager": [],
                "jobs_sort": [],
                "jobs_pagination": [],
            },
            "frozen_facets": {},
            "http_params": {
                "get": {
                    "_paged": "1",
                },
                "uri": "positions",
                "url_vars": [],
                "lang": "en",
            },
            "template": "wp",
            "extras": {
                "sort": "default",
            },
            "soft_refresh": 1,
            "is_bfcache": 1,
            "first_load": 0,
            "paged": "1",
        },
    }
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()

    def start_requests(self):
        yield scrapy.Request(
            "https://careers.airbnb.com/positions/?_paged=1",
            headers=self.headers,
            body=json.dumps(self.json_data),
            method="POST",
        )

    def parse(self, response):
        seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        jobs = scrapy.Selector(text=json.loads(response.body).get("template")).xpath(
            '//div[contains(@class,"inner-grid")]/div[not(contains(.,"Legal"))]/h3'
        )
        total_jobs = len(jobs)
        self.remove_obsolete_jobs = self.max_jobs >= total_jobs

        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            job_dict = {
                "title": job.xpath("./a/text()").get(),
                "url": job.xpath("./a/@href").get(),
                "apply_link": job.xpath("./a/@href").get(),
            }
            job_dict = defaultdict(lambda: None, job_dict)
            id_unique = get_id_unique(self.name, job_dict, title=job_dict["title"])
            job_dict["id_unique"] = id_unique
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1
            yield scrapy.Request(
                url=job_dict["url"],
                headers=self.headers,
                callback=self.job_detail,
                meta={
                    "job_dict": job_dict,
                    "remote": (
                        True
                        if job.xpath(
                            './preceding-sibling::div[contains(.,"Live and Work Anywhere")]'
                        )
                        else False
                    ),
                },
            )

        total_page = (
            json.loads(response.body)
            .get("settings", {})
            .get("pager", {})
            .get("total_pages")
        )
        current_page = (
            json.loads(response.body).get("settings", {}).get("pager", {}).get("page")
        )
        if current_page < total_page and (self.fetched_count < self.max_jobs):
            next_page = current_page + 1
            data = deepcopy(self.json_data)
            data["data"]["paged"] = str(next_page)
            data["data"]["http_params"]["get"]["_paged"] = str(next_page)
            yield scrapy.Request(
                f"https://careers.airbnb.com/positions/?_paged={next_page}",
                headers=self.headers,
                body=json.dumps(data),
                method="POST",
            )
        else:
            self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")

    def job_detail(self, response):
        further_info = response.meta["job_dict"]
        further_info["location"] = [
            country.split(",")[0].strip() if "," in country else country.strip()
            for country in response.css("div.offices>span::text").getall()
        ]
        if response.meta["remote"]:
            further_info["location"].insert(0, "Remote")
        further_info["description"] = markdownify(response.css("div.job-detail").get())
        job_posting_text = f"""Job title:\n {further_info['title']}
                                Description:\n{further_info['description']}
                                """
        job_info = get_job_info(job_posting_text)
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(AirbnbSpider)
    process.start()
