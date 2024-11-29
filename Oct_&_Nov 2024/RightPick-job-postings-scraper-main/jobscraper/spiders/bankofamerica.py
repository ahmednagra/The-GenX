import json
import os
from collections import defaultdict
from datetime import datetime
from urllib.parse import urljoin
import scrapy
from markdownify import markdownify
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from jobscraper.spiders import close_spider

MAX_JOBS = 100_000


class BankOfAmericaSpider(scrapy.Spider):
    name = "bankofamerica"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    url = (
        "https://careers.bankofamerica.com/services/jobssearchservlet?filters=division=Global%20Corporate%20AND%20In"
        "vestment%20Banking,division=Global%20Markets,division=Global%20Technology&start={}&rows={}&search=getAllJobs"
    )
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    def start_requests(self):
        self.max_jobs = getattr(self, "max_jobs", MAX_JOBS)
        self.max_jobs = int(self.max_jobs)
        self.seen_jobs_set = get_seen_jobs(self.name)
        yield scrapy.Request(
            self.url.format(0, 10),
            headers=self.headers,
            meta={"offset": 0, "url": self.url, "rows": 10},
        )

    def parse(self, response, **kwargs):
        offset = response.meta.get("offset")
        rows = response.meta.get("rows")
        data = json.loads(response.body)
        total_records = data.get("totalMatches")
        self.remove_obsolete_jobs = self.max_jobs >= total_records
        for job in data.get("jobsList", []):
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            job_dict = {
                "title": job.get("postingTitle", ""),
                "id": job.get("jobRequisitionId", ""),
                "url": urljoin(
                    "https://careers.bankofamerica.com/", job.get("jcrURL", "")
                ),
                "postedDate": job.get("postedDate"),
                "brand": job.get("brand"),
                "family": job.get("family"),
                "lob": job.get("lob"),
                "area": job.get("area"),
                "subLob": job.get("subLob"),
                "postingSite": job.get("postingSite"),
                "travelRequired": job.get("travelRequired"),
                "yearsOfExperience": job.get("yearsOfExperience"),
                "workShift": job.get("workShift"),
                "minYearsOfExperience": job.get("minYearsOfExperience"),
            }
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
            yield scrapy.Request(
                job_dict["url"], callback=self.detail, meta={"item": job_dict}
            )
        offset += 10
        if offset < total_records and self.fetched_count < self.max_jobs:
            rows += 10
            yield scrapy.Request(
                response.meta.get("url").format(offset, rows),
                headers=self.headers,
                meta={"offset": offset, "url": response.meta.get("url"), "rows": rows},
            )
        else:
            self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")

    def detail(self, response):
        locations = response.xpath(
            '//div[contains(@class,"locations__names")]//span/text()'
        ).getall()
        locations2 = []
        for location in locations:
            if ";" in location:
                locations2.extend(location.split(";"))
            else:
                locations2.append(location)
        # Below is commented because of cases like: locations2 = ['Chennai, , India', 'Link Road Malad (West), Mumbai', 'Gf, Hyderabad']
        # cities = []
        # for location in locations2:
        #     if "," in location:
        #         cities.append(location.split(",")[0].strip())
        #     else:
        #         if location:
        #             cities.append(location.strip())
        cities = locations2
        job_dict = response.meta["item"]
        job_dict["location"] = cities
        job_dict["job_type"] = response.xpath(
            '//p[@class="item job-information__type"]/span/text()'
        ).get("")
        job_dict["info_id"] = response.xpath(
            '//p[@class="item job-information__id"]/span/text()'
        ).get("")
        job_dict["apply_link"] = response.xpath("//a[text()='Apply']/@href").get("")
        job_dict["description"] = markdownify(
            response.css("div.job-description-body__internal").get("")
        )
        job_posting_text = f"""Job title:\n {job_dict['title']}
                        Description:\n{job_dict['description']}
                        """
        job_info = get_job_info(job_posting_text)
        yield {**job_dict, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(BankOfAmericaSpider)
    process.start()
