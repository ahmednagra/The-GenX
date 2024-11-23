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


class NetflixSpider(scrapy.Spider):
    name = "netflix"
    start_url = "https://explore.jobs.netflix.net/api/apply/v2/jobs?domain=netflix.com&start={}&num=10&exclude_pid=790298289882&pid=790298289882&Teams=Engineering%20Operations&Teams=Games&Teams=Product&Teams=Program%20Management&Teams=Promotional%20Content&Teams=Promotional%20Creative%20Production&Teams=Sales%20and%20Business%20Development&Teams=Supply%20Chain&domain=netflix.com&sort_by=relevance"
    job_url = "https://explore.jobs.netflix.net/api/apply/v2/jobs/{}?domain=netflix.com&Teams=Engineering+Operations&Teams=Games&Teams=Product&Teams=Program+Management&Teams=Promotional+Content&Teams=Promotional+Creative+Production&Teams=Sales+and+Business+Development&Teams=Supply+Chain&sort_by=relevance"
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
    run_completed = False

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        yield scrapy.Request(url=self.start_url.format(10), meta={"start": 10})

    def parse(self, response):
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)

        data = json.loads(response.text)
        total_jobs = data.get("count", 0)
        self.remove_obsolete_jobs = self.max_jobs >= total_jobs

        jobs = data.get("positions", [])
        for job in jobs:
            # job_example = {
            #     "id": 790298595070,
            #     "name": "Director, Marketing Partnerships - Korea",
            #     "location": "Seoul,Korea, Republic of",
            #     "locations": [
            #         "Seoul,Korea, Republic of"
            #     ],
            #     "hot": 0,
            #     "department": "Promotional Content",
            #     "business_unit": "Streaming",
            #     "t_update": 1724198400,
            #     "t_create": 1724198400,
            #     "ats_job_id": "JR29331",
            #     "display_job_id": "JR29331",
            #     "type": "ATS",
            #     "id_locale": "JR29331-en-US",
            #     "job_description": "",
            #     "locale": "en-US",
            #     "stars": 0,
            #     "medallionProgram": None,
            #     "location_flexibility": None,
            #     "work_location_option": "onsite",
            #     "canonicalPositionUrl": "https://explore.jobs.netflix.net/careers/job/790298595070",
            #     "isPrivate": False
            # }
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            position_id = str(job.get("id"))
            job_dict = {
                "id": position_id,
                "title": job.get("name").strip(),
            }
            job_dict = defaultdict(lambda: None, job_dict)
            id_unique = get_id_unique(self.name, job_dict)
            job_dict["id_unique"] = id_unique
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in self.seen_jobs_set:
                self.logger.info(f'👀 Job "{job_dict["title"]}" already seen. Skipping...')
                continue
            self.fetched_count += 1

            locations = job.get("locations", [])
            location_list = []
            for loc in locations:
                if " - " in loc:
                    parts = loc.split(" - ")
                    if parts[-1] == "Remote":
                        # Example: "USA - Remote" stays as "USA - Remote"
                        location_list.append(loc)
                    else:
                        # Example: "New York - Los Angeles, USA" becomes ["New York", "Los Angeles, USA"]
                        location_list.extend(parts)
                else:
                    # Example: "London" stays as "London"
                    location_list.append(loc)

            job_dict_extra = {
                "id_unique": id_unique,
                "url": f"https://explore.jobs.netflix.net/careers?pid={job.get('id')}&domain=netflix.com&sort_by=relevance",
                "location": location_list,
                "description": job.get("job_description"),
                "department": job.get("department"),
                "business_unit": job.get("business_unit"),
                "type": job.get("type"),
                "locale": job.get("locale"),
                "stars": job.get("stars"),
                "location_flexibility": job.get("location_flexibility"),
                "work_location_option": job.get("work_location_option"),
                "canonicalPositionUrl": job.get("canonicalPositionUrl"),
                **job_dict
            }
            job_dict_extra = defaultdict(lambda: None, job_dict_extra)

            yield scrapy.Request(
                url=self.job_url.format(position_id),
                callback=self.parse_job,
                meta={"job_dict": job_dict_extra},
            )

        if self.fetched_count < self.max_jobs and jobs:
            page = response.meta["start"] + 10
            yield scrapy.Request(url=self.start_url.format(page), meta={"start": page})
        else:
            self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")

    def parse_job(self, response):
        job = json.loads(response.text)
        job_dict = response.meta["job_dict"]
        job_dict_extra = {
            "description": markdownify(job.get("job_description")),
            "department": job.get("department"),
            "organization": "".join(job.get("organization", [])),
            "team": "".join(job.get("team", [])),
        }
        if job.get("organization"):
            job_dict_extra["organization"] = job.get("organization")
        if job.get("state"):
            job_dict_extra["state"] = job.get("state")
        if job.get("lever_team"):
            job_dict_extra["lever_team"] = job.get("lever_team")

        job_posting_text = f"""Job title:\n {job_dict['title']}
        Description:\n{job_dict['description']}
        """
        job_info = get_job_info(job_posting_text)
        yield {**job_dict, **job_dict_extra, **job_info}

    def closed(self, reason):
        close_spider(self, reason)

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(NetflixSpider)
    process.start()
