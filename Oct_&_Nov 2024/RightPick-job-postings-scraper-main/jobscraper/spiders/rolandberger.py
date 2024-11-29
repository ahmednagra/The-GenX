import json
import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
import os
from jobscraper.spiders import close_spider


MAX_JOBS = 1_000_000


class RolandBergerSpider(scrapy.Spider):
    name = "rolandberger"
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
        urls = [
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Assistenz+%28Corporate+Functions%29%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Automotive+%26+Industrials%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Chemicals%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Civil+Economics%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Construction%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Consumer+Goods%2C+Retail+%26+Agribusiness%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Corporate+Finance%2C+Restructuring+%26+Private+Equity%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Digital%2C+Tech+%26+Analytics%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Energy+%26+Utilities%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Financial+Services%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22IT+%28Corporate+Functions%29%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Infrastructure%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Media+%26+Telco%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Operations%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Other%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Pharma+%26+Healthcare%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Sustainability%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Transformation+%26+Organization%22",
            "https://rolandberger-search-api.e-spirit.cloud/v1/prepared_search/JoinJobs/execute/?language=en&query=*&facet.filter.hr_expertise=%22Transportation%2C+Tourism%2C+Logistics%22",
        ]
        for url in urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={"sops_keep_headers": True},
                errback=self.handle_error,
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
        if response.status == 500:
            self.logger.warning(f"Received 500 error from {response.url}. Skipping...")
            return  # Skip processing the response

        data = json.loads(response.text)
        results = data.get("results", [])
        if results:
            for job in results:
                if self.fetched_count >= self.max_jobs:
                    self.logger.info(
                        f"🛑 Reached max jobs: {self.max_jobs}. Stopping..."
                    )
                    return
                job_dict = {
                    "id": job.get("id"),
                    "url": job.get("link"),
                    "apply_link": job.get("positionProfile___webAddress")[0],
                    "title": job.get("title")[0],
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
                if job_dict.get("url") == "https://rolandberger.jobs/en/1179":
                    continue
                yield scrapy.Request(
                    job_dict.get("url"),
                    self.detail_page,
                    meta={"job_dict": job_dict},
                    errback=self.handle_error,
                )

    def detail_page(self, response):
        try:
            if response.status != 200:
                self.logger.warning(
                    f"Skipping non-200 response from {response.url}. Status: {response.status}"
                )
                return  # Skip processing if the response status is not OK

            job_dict = response.meta.get("job_dict")
            id_unique = job_dict.get("id_unique")
            job_title = job_dict.get("title")
            job_id = job_dict.get("id")
            apply_link = job_dict.get("apply_link")
            job_url = job_dict.get("url")
            data = response.css('[type="application/ld+json"]::text').get()
            if data:
                data = json.loads(data)
                datePosted = data.get("datePosted")
                description = data.get("description")
                locations = [
                    place["address"]["addressLocality"]
                    for place in data["jobLocation"]
                    if place["address"]["addressLocality"] is not None
                ]

                further_info = {
                    "id_unique": id_unique,
                    "title": job_title,
                    "url": job_url,
                    "id": job_id,
                    "posted_date": datePosted,
                    "location": locations,
                    "apply_link": apply_link,
                    "description": markdownify(description),
                }
                job_posting_text = f"""Job title:\n {further_info['title']}
                                        Description:\n{further_info['description']}
                                        """
                job_info = get_job_info(job_posting_text)
                yield {**further_info, **job_info}
        except Exception as e:
            self.logger.info(f"Error processing page: {response.url}, {str(e)}")
            pass  # Silently suppress exceptions to avoid stopping the spider

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(RolandBergerSpider)
    process.start()
