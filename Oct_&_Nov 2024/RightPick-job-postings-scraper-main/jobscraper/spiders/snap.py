import json
import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
import os
from jobscraper.spiders import close_spider

MAX_JOBS = 1_000_000


class SnapSpider(scrapy.Spider):
    name = "snap"
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
        referers = [
            "Augmented+Reality",
            "Engineering",
            "Growth",
            "Marketing",
            "Operations",
            "Product",
            "Snap+Product+R%26D",
            "Snap+Lab",
            "Sales",
        ]
        urls = [
            "https://careers.snap.com/api/jobs?location=&role=&team=Augmented+Reality&type=",
            "https://careers.snap.com/api/jobs?location=&role=&team=Engineering&type=",
            "https://careers.snap.com/api/jobs?location=&role=&team=Growth&type=",
            "https://careers.snap.com/api/jobs?location=&role=&team=Marketing&type=",
            "https://careers.snap.com/api/jobs?location=&role=&team=Operations&type=",
            "https://careers.snap.com/api/jobs?location=&role=&team=Product&type=",
            "https://careers.snap.com/api/jobs?location=&role=&team=Snap+Product+R%26%2338%3BD&type=",
            "https://careers.snap.com/api/jobs?location=&role=&team=Snap+Lab&type=",
            "https://careers.snap.com/api/jobs?location=&role=&team=Sales&type=",
        ]
        for url, ref in zip(urls, referers):
            yield scrapy.Request(
                url, callback=self.parse, meta={"ref": ref}, errback=self.handle_error
            )

    def handle_error(self, failure):
        # log all failures
        self.logger.error(repr(failure))
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error("❌ HttpError on %s", response.url)
            self.logger.error("⛑️ Response body:\n%s", response.body)
            self.logger.error("🔎 Request headers:\n%s", response.request.headers)
            # self.logger.error('🔎 Request body:\n%s', response.request.body)

    def parse(self, response):
        ref = response.meta.get("ref")
        data = json.loads(response.text)
        jobs = data.get("body")
        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            source = job.get("_source")
            emp_type = source.get("employment_type")
            role = source.get("role")
            location = []
            locations = source.get("offices")
            if locations:
                for loc in locations:
                    city = loc.get("name")
                    location.append(city)
            url = source.get("absolute_url")
            department = source.get("departments")
            id = source.get("id")
            title = source.get("title")
            jobPostingSite = source.get("jobPostingSite")
            job_dict = {
                "id": str(id),
                "url": url,
                "title": title,
            }
            id_unique = get_id_unique(
                self.name, job_dict, id=str(job_dict["id"]), title=job_dict["title"]
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
            headers = {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                # 'cookie': 'sc-wcid=0466fc97-72bb-4cbf-8ad3-5af3efdce6ac; sc-cookies-accepted=true; EssentialSession=true; Preferences=true; Performance=true; Marketing=true; blizzard_client_id=d9b65c23-981c-450c-94cf-5f26786dd2ae:1727859547130; _ga=GA1.1.802258249.1727859564; blizzard_web_session_id=mQgdhVHqvfu4Hoed; _ga_GKBE1PEVE6=GS1.1.1728162647.6.1.1728165559.0.0.0',
                "ect": "4g",
                "priority": "u=1, i",
                "referer": f"https://careers.snap.com/jobs?team={ref}",
                "sec-ch-prefers-reduced-motion": "no-preference",
                "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            }
            yield scrapy.Request(
                url,
                callback=self.detail_page,
                headers=headers,
                meta={
                    "sops_keep_headers": True,
                    "emp_type": emp_type,
                    "role": role,
                    "location": location,
                    "department": department,
                    "id": id,
                    "title": title,
                    "jobPostingSite": jobPostingSite,
                },
                errback=self.handle_error,
            )

    def detail_page(self, response):
        emp_type = response.meta.get("emp_type")
        role = response.meta.get("role")
        location = response.meta.get("location")
        department = response.meta.get("department")
        id = response.meta.get("id")
        title = response.meta.get("title")
        jobPostingSite = response.meta.get("jobPostingSite")
        data = response.css('[type="application/ld+json"]::text').get()
        data = json.loads(data)
        datePosted = data.get("datePosted")
        description = data.get("description")
        further_info = {
            "id": str(id),
            "title": title,
            "department": department,
            "job_posting_site": jobPostingSite,
            "location": location,
            "date_posted": datePosted,
            "description": description,
            "role": role,
            "emp_type": emp_type,
        }
        job_posting_text = f"""Job title:\n {further_info['title']}
                                                Description:\n{further_info['description']}
                                                """
        job_info = get_job_info(job_posting_text)
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(SnapSpider)
    process.start()
