import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
import os
from jobscraper.spiders import close_spider
from markdownify import markdownify
import json

MAX_JOBS = 1_000_000


class JaneStreetSpider(scrapy.Spider):
    name = "janestreet"
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
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        self.remove_obsolete_jobs = self.max_jobs >= MAX_JOBS
        url = "https://www.janestreet.com/jobs/main.json"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        jobs = json.loads(response.text)
        required_cats = [
            "Trading, Research, and Machine Learning",
            "Technology",
            "Institutional Sales and Trading",
            "Strategy and Product",
        ]
        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            location = []
            if job.get("category") in required_cats and (
                "Full-Time: Experienced" in job.get("availability")
                or "Full-Time: New Grad" in job.get("availability")
                or "Internship" in job.get("availability")
                or "Co-Op" in job.get("availability")
            ):
                id = job.get("id")  # int
                title = job.get("position")
                department = job.get("category")
                availability = job.get("availability")
                city = job.get("city")
                if city:
                    if city == "HKG":
                        location.append("Hong Kong")
                    elif city == "SGP":
                        location.append("Singapore")
                    elif city == "LDN":
                        location.append("London")
                    elif city == "NYC":
                        location.append("New York City")
                    else:
                        location.append(city)
                description = job.get("overview")
                description = markdownify(description)
                apply_link = f"https://www.janestreet.com/join-jane-street/apply/{id}/"
                url = "https://www.janestreet.com/join-jane-street/position/" + str(id)
                team = job.get("team")
                duration = job.get("duration")
                max_salary = job.get("max_salary")
                min_salary = job.get("min_salary")
                job_dict = {
                    "id": str(id),
                    "url": url,
                    "title": title,
                }
                id_unique = get_id_unique(
                    self.name, job_dict, id=job_dict["id"], title=job_dict["title"]
                )
                self.scraped_jobs_dict[id_unique] = job_dict["title"]
                self.seen_jobs_count += 1
                if id_unique in self.seen_jobs_set:
                    self.logger.info(
                        f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                    )
                    continue
                self.fetched_count += 1
                further_info = {
                    "id": str(id),
                    "url": url,
                    "title": title,
                    "department": department,
                    "availability": availability,
                    "location": location,
                    "description": description,
                    "team": team,
                    "apply_link:": apply_link,
                    "duration": duration,
                    "min_salary": min_salary,
                    "max_salary": max_salary,
                }
                job_posting_text = f"""Job title:\n {further_info['title']}
                Description:\n{further_info['description']}
                Minimum Salary:\n{min_salary}
                Maximum Salary:\n{max_salary}
                """
                job_info = get_job_info(job_posting_text)

                yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(JaneStreetSpider)
    process.start()
