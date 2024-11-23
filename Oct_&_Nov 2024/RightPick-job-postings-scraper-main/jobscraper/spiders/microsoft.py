import json
import os
from collections import defaultdict
from datetime import datetime
from math import ceil
import scrapy
from markdownify import markdownify
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from jobscraper.spiders import close_spider


class MicrosoftSpider(scrapy.Spider):
    name = "microsoft"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    run_completed = False
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,de;q=0.8",
        "authorization": "Bearer undefined",
        "origin": "https://jobs.careers.microsoft.com",
        "referer": "https://jobs.careers.microsoft.com/",
        "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "x-correlationid": "8bbaf110-910e-01ab-c382-7cb254a3ce58",
        "x-subcorrelationid": "b8fc2d56-3958-c297-7cf9-f5e41cf4a7f3",
    }
    # url = "https://gcsservices.careers.microsoft.com/search/api/v1/search?l=en_us&pg={}&pgSz=20&o=Relevance&flt=true"
    url = "https://gcsservices.careers.microsoft.com/search/api/v1/search?p=Analytics&p=Business%20Development%20%26%20Ventures&p=Business%20Operations&p=Customer%20Success&p=Digital%20Sales%20and%20Solutions&p=Finance&p=Hardware%20Engineering&p=Marketing&p=Product%20Management&p=Program%20Management&p=Quantum%20Computing&p=Research%2C%20Applied%2C%20%26%20Data%20Sciences&p=Sales&p=Software%20Engineering&p=Technology%20Sales&l=en_us&pg={}&pgSz=20&o=Relevance&flt=true"

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        yield scrapy.Request(self.url.format(1), headers=self.headers, meta={"page": 1})

    def parse(self, response, **kwargs):
        data = json.loads(response.body)
        total_jobs = data.get("operationResult", {}).get("result", {}).get("totalJobs")
        self.max_jobs = getattr(self, "max_jobs", total_jobs)
        self.max_jobs = int(self.max_jobs)
        self.remove_obsolete_jobs = self.max_jobs >= total_jobs
        jobs = data.get("operationResult", {}).get("result", {}).get("jobs", [])
        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            url = "https://jobs.careers.microsoft.com/global/en/job/{}/{}".format(
                job.get("jobId"), job.get("title", "").replace(" ", "-")
            )
            job_dict = {
                "title": job.get("title", ""),
                "id": job.get("jobId"),
                "url": url,
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
            json_url = "https://gcsservices.careers.microsoft.com/search/api/v1/job/{}?lang=en_us".format(
                job_dict["id"]
            )
            yield scrapy.Request(
                url=json_url,
                headers=self.headers,
                callback=self.detail,
                meta={"job_dict": job_dict},
            )

        page = response.meta.get("page")
        total_pages = ceil(total_jobs / 20)
        if page < total_pages and self.fetched_count < self.max_jobs:
            page += 1
            yield scrapy.Request(
                self.url.format(page), headers=self.headers, meta={"page": page}
            )
        else:
            self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")

    def detail(self, response):
        data = json.loads(response.body).get("operationResult", {}).get("result", {})
        job_dict = response.meta.get("job_dict")
        job_dict["job_type"] = [data.get("employmentType", "")]
        job_dict["location"] = [i.get("city") for i in data.get("workLocations", [])]
        job_dict["location"].append(data.get("primaryWorkLocation", {}).get("city", ""))
        job_dict["location"] = list(set(job_dict["location"]))
        job_dict["description"] = markdownify(data.get("description", ""))
        job_dict["posted_date"] = datetime.strptime(
            data.get("posted", {}).get("external", ""), "%Y-%m-%dT%H:%M:%S"
        ).strftime("%b %d, %Y")
        job_dict["work_site_flexibility"] = data.get("workSiteFlexibility", "")
        job_dict["travel_percentage"] = data.get("travelPercentage", "")
        job_dict["role_type"] = data.get("roleType", "")
        job_dict["category"] = data.get("category", "")
        job_dict["subcategory"] = data.get("subcategory", "")
        job_dict["education_level"] = data.get("educationLevel", "")
        if data.get("workSiteFlexibility", "") == "Up to 100% work from home":
            job_dict["location"].insert(0, "Remote")
        job_posting_text = f"""Job title:\n {job_dict['title']}
                        Description:\n{job_dict['description']}
                        """
        job_info = get_job_info(job_posting_text)
        yield {**job_dict, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MicrosoftSpider)
    process.start()
