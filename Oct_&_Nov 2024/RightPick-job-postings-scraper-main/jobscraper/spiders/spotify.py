import html
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


class SpotifySpider(scrapy.Spider):
    name = "spotify"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "dnt": "1",
        "origin": "https://www.lifeatspotify.com",
        "priority": "u=1, i",
        "referer": "https://www.lifeatspotify.com/",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    url = "https://api-dot-new-spotifyjobs-com.nw.r.appspot.com/wp-json/animal/v1/job/search?c=data-insights-leadership%2Cdata-science%2Cuser-research%2Cdesign-ops%2Ceditorial-design%2Cinternal-tools-design%2Cproduct-design%2Cux-writing%2Cbackend%2Cclient-c%2Cdata%2Cdeveloper-tools-infrastructure%2Cengineering-leadership%2Cmachine-learning%2Cmobile%2Cnetwork-engineering-it%2Csecurity%2Ctech-research%2Cweb%2Cbrand-creative-marketing%2Cconsumer-marketing%2Ccontent-marketing%2Cmarketing-analytics%2Cmarketing-operations%2Cproduct%2Cad-sales%2Cadvertising-marketing-sales%2Cbusiness-development%2Cpartnerships%2Csales-operations%2Csubscriptions"
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    run_completed = False

    def start_requests(self):
        yield scrapy.Request(url=self.url, headers=self.headers)

    def parse(self, response):
        seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        jobs = json.loads(response.body).get("result")
        total_jobs = len(jobs)
        self.remove_obsolete_jobs = self.max_jobs >= total_jobs

        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            url = f'https://lifeatspotify.com/jobs/{job.get("id")}'
            job_dict = {
                "id": job.get("id"),
                "url": url,
                "apply_link": url,
                "title": job.get("text"),
                "main_category": job.get("main_category", {}).get("name"),
                "sub_category": job.get("sub_category", {}).get("name"),
                "job_type": job.get("job_type", {}).get("name"),
                "location": [city.get("location") for city in job.get("locations", [])],
            }
            job_dict = defaultdict(lambda: None, job_dict)
            id_unique = get_id_unique(
                self.name, job_dict, id=str(job_dict["id"]), title=job_dict["title"]
            )
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
                url=url,
                headers=self.headers,
                callback=self.job_detail,
                meta={"job_dict": job_dict},
            )

    def job_detail(self, response):
        further_info = response.meta["job_dict"]
        further_info["description"] = markdownify(
            response.css("div.singlejob_maxWidth__0SwoF").get()
        )
        job_posting_text = f"""Job title:\n {further_info['title']}
                                Description:\n{further_info['description']}
                                """
        job_info = get_job_info(job_posting_text)
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(SpotifySpider)
    process.start()
