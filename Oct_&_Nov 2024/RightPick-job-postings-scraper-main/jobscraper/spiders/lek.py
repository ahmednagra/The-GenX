import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from collections import defaultdict
import os
from jobscraper.spiders import close_spider
import re
MAX_JOBS = 1_000_000


class LEKSpider(scrapy.Spider):
    name = "lek"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    urls = [
        "https://lek.tal.net/vx/lang-en-GB/mobile-0/system-42/appcentre-1/brand-2/candidate/jobboard/vacancy/1/adv/?f_Item_Opportunity_451_lk=689",
        "https://lek.tal.net/vx/lang-en-GB/mobile-0/system-42/appcentre-2/brand-2/candidate/jobboard/vacancy/3/adv/?f_Item_Opportunity_451_lk=689",
        "https://lek.tal.net/vx/lang-en-GB/mobile-0/appcentre-3/brand-2/candidate/jobboard/vacancy/5/adv/",
        "https://lek.tal.net/vx/lang-en-GB/mobile-0/system-42/appcentre-4/brand-2/candidate/jobboard/vacancy/7/adv/?f_Item_Opportunity_451_lk=689",
        "https://lek.tal.net/vx/lang-en-GB/mobile-0/appcentre-middle_east/brand-2/spa-1/candidate/jobboard/vacancy/11/adv/",
    ]
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    run_completed = False

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        for url in self.urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        jobs = response.css('.candidate-opp-tile')
        total_jobs = len(jobs)
        self.remove_obsolete_jobs = self.max_jobs >= total_jobs

        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            url = job.css('.subject::attr(href)').get()

            # Extract the stable part of the URL
            match = re.search(r'/opp/(\d+-[^/]+)', url)
            end_of_url = match.group(1) if match else url

            job_dict = {
                "url": url,
                "apply_link": url,
                "title": job.css('.subject::text').get().strip(),
                "end_of_url": end_of_url,
            }
            job_dict = defaultdict(lambda: None, job_dict)
            id_unique = get_id_unique(
                self.name, job_dict, title=job_dict["title"], id=job_dict["end_of_url"]
            )
            job_dict["id_unique"] = id_unique
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in self.seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1
            
            job_dict["location"] = job.css('.candidate-opp-field-3::text').get().strip().split(', ')
            
            yield scrapy.Request(
                url=url,
                callback=self.job_detail,
                meta={"job_dict": job_dict},
            )

    def job_detail(self, response):
        further_info = response.meta["job_dict"]
        further_info["description"] = markdownify(
            response.css(".item23 .form-control-static").get()
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
    process.crawl(LEKSpider)
    process.start()
