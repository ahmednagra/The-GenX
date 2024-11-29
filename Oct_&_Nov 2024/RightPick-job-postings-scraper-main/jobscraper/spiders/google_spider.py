import sys
from typing import Iterable
import scrapy
from datetime import datetime
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from markdownify import markdownify
from collections import defaultdict
import os

from jobscraper.spiders import close_spider
from dataextraction import get_job_info, get_seen_jobs, get_id_unique


MAX_JOBS = 1_000_000


class GoogleSpider(scrapy.Spider):
    name = "google"

    urls = [
        "https://www.google.com/about/careers/applications/jobs/results/?distance=50&q=Business&hl=en_US",
        "https://www.google.com/about/careers/applications/jobs/results/?distance=50&q=Technology&hl=en_US",
    ]

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

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        self.remove_obsolete_jobs = self.max_jobs >= MAX_JOBS
        for url in self.urls:
            yield scrapy.Request(url, callback=self.parse, meta={'start_url': url})

    def parse(self, response):
        jobs = response.xpath('//ul[@class="spHGqe"]/li/div')
        total_jobs_on_page = len(jobs)
        
        # Check if there are no results
        if total_jobs_on_page == 0:
            self.logger.info(f"🛑 No more results found for {response.meta['start_url']}. Stopping...")
            return

        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            job_dict = {
                "title": job.xpath('.//h3[@class="QJPWVe"]/text()').get("").strip(),
                "url": job.xpath(
                    './/a[contains(@aria-label,"Learn more about")]/@href'
                ).get(),
            }
            # Fix the job URL
            if job_dict["url"] and not job_dict["url"].startswith("http"):
                job_dict["url"] = (
                    f"https://www.google.com/about/careers/applications/{job_dict['url']}"
                )

            job_dict = defaultdict(lambda: None, job_dict)
            id_unique = get_id_unique(self.name, job_dict)
            job_dict["id_unique"] = id_unique
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in self.seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1
            yield response.follow(
                url=job_dict["url"],
                callback=self.parse_job,
                meta={"job_dict": job_dict, 'start_url': response.meta['start_url']},
            )

        next_page = response.css('a[aria-label="Go to next page"]::attr(href)').get("")
        if self.fetched_count < self.max_jobs and next_page:
            self.logger.info(f"🔗 Moving to next page: {next_page}")
            self.logger.info(f'🧮🧮🧮 Seen jobs count: {self.seen_jobs_count} | Fetched count: {self.fetched_count} | Total count on page: {total_jobs_on_page} | Next page: {next_page} | max_jobs: {self.max_jobs}')
            yield response.follow(url=next_page, callback=self.parse, meta=response.meta)
        else:
            self.logger.info(f"🛑 Reached max jobs (max_jobs = {self.max_jobs}) or end of search results for {response.meta['start_url']}. Stopping...")
            return

    def parse_job(self, response):
        job_dict = response.meta["job_dict"]
        cities = []
        locations_check = " ".join(
            response.xpath(
                '//span[@class="MyVLbf" and contains(.,"working location from the following:")]/b//text()'
            ).getall()
        )
        for locations_type in response.xpath(
            '//span[@class="MyVLbf" and contains(.,"working location from the following:")]/b'
        ):
            all_cities = locations_type.xpath("./text()").get("")
            if all_cities:
                for city in all_cities.split(";"):
                    if "In-office" in locations_check and "Remote" in locations_check:
                        if "In-office" in all_cities.split(";")[0]:
                            city = (
                                city.split(":", 1)[-1]
                                .split(",")[0]
                                .strip()
                                .replace(".", "")
                            )
                            cities.append(city)
                        elif "Remote" in all_cities.split(";")[0] and len(cities) > 0:
                            city = (
                                city.split(":", 1)[-1]
                                .split(",")[-1]
                                .strip()
                                .replace(".", "")
                            )
                            if f"Remote, {city}" not in cities:
                                cities.append(f"Remote, {city}")
                    else:
                        if "Remote" in city:
                            city = (
                                city.split(":", 1)[-1]
                                .split(",")[0]
                                .strip()
                                .replace(".", "")
                            )
                            cities.append("Remote")
                            cities.append(city)
                        else:
                            city = city.split(",")[0].strip().replace(".", "")
                            cities.append(city)

        if not cities:
            for location in response.css("span.pwO9Dc.vo5qdf>span"):
                city = location.css("::text").get("")
                if city:
                    cities.append(city.replace(";", "").strip().split(",")[0].strip())
        further_info = {
            "location": cities,
            "organization": response.css("span.RP7SMd>span::text").get("").strip(),
            "experience_level": response.css("span.wVSTAb::text").get("").strip()
            or response.xpath(
                '//i[contains(text(),"bar_chart")]/following-sibling::span/text()'
            )
            .get("")
            .strip(),
            "apply_link": f"https://www.google.com/about/careers/applications/{response.css('a#apply-action-button::attr(href)').get('').strip()}",
            "minimum_qualifications": markdownify(
                "".join(
                    response.xpath(
                        '//h3[contains(text(),"Minimum qualifications:")]/following-sibling::ul[following-sibling::h3]'
                    ).getall()
                )
            ),
            "preferred_qualifications": markdownify(
                "".join(
                    response.xpath(
                        '//h3[contains(text(),"Preferred qualifications:")]/following-sibling::ul'
                    ).getall()
                )
            ),
            "description": markdownify(
                response.css("div.aG5W3").get("")
                or "".join(
                    response.xpath(
                        '//h3[contains(text(),"About the job")]/following-sibling::p'
                    ).getall()
                )
            ),
            "responsibilities": markdownify(
                response.css("div.BDNOWe").get("")
                or response.xpath(
                    '//h3[contains(text(),"Responsibilities")]/following::ul[1]'
                ).get("")
            ),
            "job_type": response.xpath(
                '//span[@class="RP7SMd"]/span[contains(text(),"Remote") or contains(text(),"remote")]/text()'
            ).get(),
        }
        job_posting_text = f"""Job title:\n {job_dict['title']}
                        Description:\n{further_info['description']}
                        Minimum Qualifications:\n{further_info['minimum_qualifications']}
                        Preferred Qualifications:\n{further_info['preferred_qualifications']}
                        Experience level:\n{further_info['experience_level']}
                        Organization:\n{further_info['organization']}
                        """
        job_info = get_job_info(job_posting_text)
        further_info = defaultdict(lambda: None, {**job_dict, **further_info})
        check_job_exist = response.xpath(
            '//h2[contains(text(),"Job not found.")]//text()'
        ).getall()
        if check_job_exist:
            self.logger.info("Job not found....")
        else:
            yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(GoogleSpider)
    process.start()
