import json
import os
from collections import defaultdict
from datetime import datetime
from urllib.parse import urljoin

from markdownify import markdownify

from dataextraction import get_job_info, get_seen_jobs, get_id_unique
import scrapy
from scrapy.crawler import CrawlerProcess
from jobscraper.spiders import close_spider

MAX_JOBS = 100_000


class AmazonSpider(scrapy.Spider):
    name = "amazon"
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

    # urls = [
    #     (
    #         "https://www.amazon.jobs/en-gb/search.json?category%5B%5D=machine-learning-science&category%5B%5D=sales-ad"
    #         "vertising-account-management&category%5B%5D=buying-planning-instock-management&category%5B%5D=project-pro"
    #         "gram-product-management-non-tech&category%5B%5D=software-development&category%5B%5D=business-intelligence"
    #         "&category%5B%5D=research-science&category%5B%5D=solutions-architect&category%5B%5D=hardware-development&c"
    #         "ategory%5B%5D=marketing-pr&category%5B%5D=design&radius=24km&facets%5B%5D=normalized_country_code&facets%"
    #         "5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business"
    #         "_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=no"
    #         "rmalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&r"
    #         "esult_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&reg"
    #         "ion=&county=&query_options=&business_category%5B%5D=student-programs&category%5B%5D=machine-learning-scie"
    #         "nce&category%5B%5D=sales-advertising-account-management&category%5B%5D=buying-planning-instock-management"
    #         "&category%5B%5D=project-program-product-management-non-tech&category%5B%5D=software-development&category%"
    #         "5B%5D=business-intelligence&category%5B%5D=research-science&category%5B%5D=solutions-architect&category%5"
    #         "B%5D=hardware-development&category%5B%5D=marketing-pr&category%5B%5D=design&"
    #     ),
    #     (
    #         "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=n"
    #         "ormalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_catego"
    #         "ry&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalize"
    #         "d_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_l"
    #         "imit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&co"
    #         "unty=&query_options=&category%5B%5D=software-development&"
    #     ),
    # ]

    urls = [
        (
            "https://www.amazon.jobs/en-gb/search.json?category%5B%5D=machine-learning-science&category%5B%5D=sales-advertising-account-management&category%5B%5D=buying-planning-instock-management&category%5B%5D=project-program-product-management-non-tech&category%5B%5D=software-development&category%5B%5D=business-intelligence&category%5B%5D=research-science&category%5B%5D=solutions-architect&category%5B%5D=hardware-development&category%5B%5D=marketing-pr&category%5B%5D=design&radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&business_category%5B%5D=student-programs&category%5B%5D=machine-learning-science&category%5B%5D=sales-advertising-account-management&category%5B%5D=buying-planning-instock-management&category%5B%5D=project-program-product-management-non-tech&category%5B%5D=software-development&category%5B%5D=business-intelligence&category%5B%5D=research-science&category%5B%5D=solutions-architect&category%5B%5D=hardware-development&category%5B%5D=marketing-pr&category%5B%5D=design&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=business-intelligence&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=business-merchant-development&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=data-science&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=database-administration&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=sales-advertising-account-management&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=software-development&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=research-science&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=project-program-product-management-non-tech&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=project-program-product-management-technical&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=marketing-pr&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=economics&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=public-policy&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=public-relations&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&team_category%5B%5D=global-financial-operations&"),
        (
            "https://www.amazon.jobs/en-gb/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={}&result_limit=10&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&category%5B%5D=hardware-development&")
    ]

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    detail_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        "Connection": "keep-alive",
        "If-None-Match": 'W/"16c2d017bfd91279886aa222b8112349"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs_or_none = getattr(self, "max_jobs", None)
        self.max_jobs = getattr(self, "max_jobs", MAX_JOBS)
        self.max_jobs = int(self.max_jobs)
        for url in self.urls:
            yield scrapy.Request(
                url.format(0), headers=self.headers, meta={"offset": 0, "url": url}
            )

    def parse(self, response, **kwargs):
        data = json.loads(response.body)
        listing_url = response.meta.get("url")
        offset = response.meta.get("offset")
        offset += 10
        hits = data.get("hits")
        jobs = data.get("jobs")
        for job in jobs:
            if self.max_jobs_or_none is not None and self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            job_dict = {
                "title": job.get("title", ""),
                "id": job.get("id_icims", ""),
                "url": urljoin("https://www.amazon.jobs", job.get("job_path", ""))
            }
            id_unique = get_id_unique(self.name, job_dict)
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in self.seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1
            job_dict["id_unique"] = id_unique

            job_dict_extension = {
                "job_type": [job.get("job_schedule_type", "")],
                "posted_date": job.get("posted_date", ""),
                "job_category": job.get("job_category", ""),
                "job_family": job.get("job_family", ""),
                "description_short": markdownify(job.get("description_short", "")),
                "apply_link": job.get("url_next_step", ""),
                "company_name": job.get("company_name", ""),
                "location": [
                    json.loads(i).get("city", "") for i in job.get("locations", [])
                ],
                "business_category": job.get("business_category", ""),
                "basic_qualifications": markdownify(
                    job.get("basic_qualifications", "")
                ),
                "description": markdownify(job.get("description", "")),
                "preferred_qualifications": markdownify(
                    job.get("preferred_qualifications", "")
                ),
                "updated_time": job.get("updated_time", ""),
                "team": job.get("team", {}).get("label", ""),
                "primary_search_label": job.get("primary_search_label", ""),
            }
            job_dict = defaultdict(lambda: None, {**job_dict, **job_dict_extension})

            job_posting_text = f"""Job title:\n {job_dict['title']}
                            Description:\n{job_dict['description']}
                            Short Description:\n{job_dict['description_short']}
                            Basic Qualifications:\n{job_dict['basic_qualifications']}
                            Preferred Qualifications:\n{job_dict['preferred_qualifications']}"""
            job_info = get_job_info(job_posting_text)
            yield {**job_dict, **job_info}
        if offset < hits and self.fetched_count < self.max_jobs:
            yield scrapy.Request(
                listing_url.format(offset),
                headers=self.headers,
                meta={"offset": offset, "url": listing_url},
            )
        else:
            self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(AmazonSpider)
    process.start()
