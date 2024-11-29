import scrapy
from datetime import datetime
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from collections import defaultdict
from scrapy.spidermiddlewares.httperror import HttpError
import json
from scrapy.crawler import CrawlerProcess
import os
from jobscraper.spiders import close_spider

MAX_JOBS = 1_000_000

class GoldmanSachsSpider(scrapy.Spider):
    name = "goldmansachs"
    page_size = 250  # max allowed by the API
    # page_number = 0
    total_count_jobs = None
    total_count_internships = None
    fetched_count = 0
    seen_jobs_count = 0

    scraped_jobs_dict = dict()
    remove_obsolete_jobs = False
    run_completed = False

    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    
    # Headers for results page request
    headers_results = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://higher.gs.com",
        "priority": "u=1, i",
        "referer": "https://higher.gs.com/",
        "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "traceparent": "00-00000000000000006c5fb52c110710e8-5bed7f14a705a3d1-01",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "x-datadog-origin": "rum",
        "x-datadog-parent-id": "6624090353610105809",
        "x-datadog-sampling-priority": "1",
        "x-datadog-trace-id": "7809159479752593640",
        "x-higher-request-id": "ae024c7b-feda-44f1-bff9-12a12f4492c6",
        "x-higher-session-id": "64b69401-65c8-4473-a00e-1e6a4fc87b04",
    }

    # Headers for job page request
    headers_job = {
        "Content-Type": "application/json",
    }

    # Payload for all jobs page request
    @staticmethod
    def payload_results(page_number, page_size=page_size):
        payload = {
            "operationName": "GetRoles",
            "variables": {
                "searchQueryInput": {
                    "page": {
                        "pageSize": page_size,
                        "pageNumber": page_number,
                    },
                    "sort": {
                        "sortStrategy": "RELEVANCE",
                        "sortOrder": "DESC",
                    },
                    "filters": [
                        {
                            "filterCategoryType": "JOB_FUNCTION",
                            "filters": [
                                {
                                    "filter": "Banker - Financing Group",
                                    "subFilters": [],
                                },
                                {
                                    "filter": "Banker - Industry/Country Coverage",
                                    "subFilters": [],
                                },
                                {
                                    "filter": "Data Engineering",
                                    "subFilters": [],
                                },
                                {
                                    "filter": "Product Management",
                                    "subFilters": [],
                                },
                                {
                                    "filter": "Quantitative Engineering",
                                    "subFilters": [],
                                },
                                {
                                    "filter": "Software Engineering",
                                    "subFilters": [],
                                },
                                {
                                    "filter": "Technology Products",
                                    "subFilters": [],
                                },
                                {
                                    "filter": "Trading",
                                    "subFilters": [],
                                },
                                {
                                    "filter": "UI Engineering",
                                    "subFilters": [],
                                },
                            ],
                        },
                    ],
                    "experiences": [
                        "PROFESSIONAL",
                        "EARLY_CAREER",
                    ],
                    "searchTerm": "",
                },
            },
            "query": "query GetRoles($searchQueryInput: RoleSearchQueryInput!) {\n  roleSearch(searchQueryInput: $searchQueryInput) {\n    totalCount\n    items {\n      roleId\n      corporateTitle\n      jobTitle\n      jobFunction\n      locations {\n        primary\n        state\n        country\n        city\n        __typename\n      }\n      status\n      division\n      skills\n      jobType {\n        code\n        description\n        __typename\n      }\n      externalSource {\n        sourceId\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}",
        }
        return json.dumps(payload)
    
    # Payload for all internships page request
    @staticmethod
    def payload_internships(page_number, page_size=page_size):
        payload = {
            "operationName": "GetCampusRoles",
            "variables": {
                "searchQueryInput": {
                    "page": {
                        "pageSize": page_size,
                        "pageNumber": page_number
                    },
                    "sort": {
                        "sortStrategy": "RELEVANCE",
                        "sortOrder": "DESC"
                    },
                    "filters": [
                        {
                            "filterCategoryType": "DIVISION",
                            "filters": [
                                {
                                    "filter": "Engineering Division",
                                    "subFilters": []
                                },
                                {
                                    "filter": "Executive Office Division",
                                    "subFilters": []
                                },
                                {
                                    "filter": "Global Banking & Markets",
                                    "subFilters": []
                                },
                                {
                                    "filter": "Global Investment Research Division",
                                    "subFilters": []
                                }
                            ]
                        }
                    ],
                    "experiences": [
                        "CAMPUS"
                    ],
                    "searchTerm": ""
                }
            },
            "query": "query GetCampusRoles($searchQueryInput: RoleSearchQueryInput!) {\n  roleSearch(searchQueryInput: $searchQueryInput) {\n    totalCount\n    items {\n      roleId\n      corporateTitle\n      jobTitle\n      jobFunction\n      locations {\n        primary\n        state\n        country\n        city\n        __typename\n      }\n      status\n      division\n      skills\n      jobType {\n        code\n        description\n        __typename\n      }\n      externalSource {\n        sourceId\n        __typename\n      }\n      educationLevel\n      startDate\n      gradDegreeStartDate\n      gradDegreeEndDate\n      __typename\n    }\n    __typename\n  }\n}"
        }
        return json.dumps(payload)

    # Payload for a particular job/internship page request
    @staticmethod
    def payload_job(id):
        return json.dumps(
            {
                "operationName": "GetRoleById",
                "variables": {
                    "externalSourceId": id,
                    "externalSourceFetch": False,
                },
                "query": "query GetRoleById($externalSourceId: String!, $externalSourceFetch: Boolean) {\n  role(\n    externalSourceId: $externalSourceId\n    externalSourceFetch: $externalSourceFetch\n  ) {\n    roleId\n    corporateTitle\n    jobTitle\n    jobFunction\n    locations {\n      primary\n      state\n      country\n      city\n      __typename\n    }\n    division\n    descriptionHtml\n    jobType {\n      code\n      description\n      __typename\n    }\n    skillset\n    compensation {\n      minSalary\n      maxSalary\n      currency\n      __typename\n    }\n    applyActive\n    status\n    externalSource {\n      externalApplicationUrl\n      applyInExternalSource\n      sourceId\n      secondarySourceId\n      __typename\n    }\n    __typename\n  }\n}",
            }
        )

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        try:
            page_no = 0
            jobs_viewed = 0
            yield scrapy.Request(
                url="https://api-higher.gs.com/gateway/api/v1/graphql",
                method="POST",
                headers=self.headers_results,
                body=self.payload_results(0),
                callback=self.parse_results,
                meta={"jobs_viewed": jobs_viewed, "page_no": page_no, 'sops_keep_headers': True},
                errback=self.handle_error,
            )
        except json.JSONDecodeError as e:
            raise scrapy.exceptions.CloseSpider(f"⚠️ Invalid JSON: {e}")

        try:
            yield scrapy.Request(
                url="https://api-higher.gs.com/gateway/api/v1/graphql",
                method="POST",
                headers=self.headers_results,
                body=self.payload_internships(0),
                callback=self.parse_internship,
                meta={"page_no": 0, 'sops_keep_headers': True},
                errback=self.handle_error,
            )
        except json.JSONDecodeError as e:
            raise scrapy.exceptions.CloseSpider(f"⚠️ Invalid JSON: {e}")

    def handle_error(self, failure):
        # log all failures
        self.logger.error(repr(failure))

        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error("❌ HttpError on %s", response.url)
            self.logger.error("⛑️ Response body:\n%s", response.body)
            self.logger.error("🔎 Request headers:\n%s", response.request.headers)
            # self.logger.error('🔎 Request body:\n%s', response.request.body)

    def parse_results(self, response):
        # print(f'ℹ️ Cookies: {response.request.cookies}')
        jobs_viewed = response.meta.get("jobs_viewed")
        page_no = response.meta.get("page_no")

        data = json.loads(response.text)
        total_job_count = data.get("data", {}).get("roleSearch", {}).get("totalCount")
        items = data.get("data", {}).get("roleSearch", {}).get("items", [])
        if self.total_count_jobs is None:
            self.total_count_jobs = int(total_job_count)
            self.logger.info(f"🔍 Total jobs to view: {self.total_count_jobs}")

        for item in items:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            jobs_viewed = jobs_viewed + 1
            source_id = item.get("externalSource", {}).get("sourceId")
            status = item.get("status")
            job_dict = {"title": item.get("jobTitle"), "id": source_id}
            job_dict = defaultdict(lambda: None, job_dict)
            id_unique = get_id_unique(self.name, job_dict)
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in self.seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{item.get("jobTitle")}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1

            job_url = "https://higher.gs.com/roles/" + source_id
            job_dict["url"] = job_url
            job_dict["id_unique"] = id_unique
            job_dict["status"] = status

            yield scrapy.Request(
                url="https://api-higher.gs.com/gateway/api/v1/graphql",
                method="POST",
                headers=self.headers_job,
                body=self.payload_job(source_id),
                callback=self.parse_job,
                meta={"job": job_dict, 'sops_keep_headers': True, "is_internship": False},
            )

        if jobs_viewed < self.total_count_jobs:
            page_no += 1
            self.logger.info(f"🕵️‍♂️ Fetching page {page_no}...")
            yield scrapy.Request(
                url="https://api-higher.gs.com/gateway/api/v1/graphql",
                method="POST",
                headers=self.headers_results,
                body=self.payload_results(page_no),
                callback=self.parse_results,
                meta={"jobs_viewed": jobs_viewed, "page_no": page_no, 'sops_keep_headers': True},
                errback=self.handle_error,
            )
        else:
            self.logger.info(f"✅ All jobs have been viewed for this round of {self.total_count_jobs} jobs.")

    def parse_internship(self, response):
        data = json.loads(response.text)
        items = data.get("data", {}).get("roleSearch", {}).get("items", [])
        page_no = response.meta.get("page_no")
        # Check if there are more pages to fetch
        total_count = int(data.get("data", {}).get("roleSearch", {}).get("totalCount", 0))
        if self.total_count_internships is None:
            self.total_count_internships = int(total_count)
            self.logger.info(f"🔍 Total internships to view: {self.total_count_internships}")
        
        self.remove_obsolete_jobs = self.max_jobs >= int(self.total_count_jobs) + int(self.total_count_internships)

        for item in items:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            source_id = item.get("externalSource", {}).get("sourceId")
            job_dict = {"title": item.get("jobTitle"), "id": source_id}
            job_dict = defaultdict(lambda: None, job_dict)
            id_unique = get_id_unique(self.name, job_dict)
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            if id_unique in self.seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{item.get("jobTitle")}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1
            
            status = item.get("status")
            job_url = "https://higher.gs.com/roles/" + source_id
            job_dict["url"] = job_url
            job_dict["id_unique"] = id_unique
            job_dict["status"] = status

            yield scrapy.Request(
                url="https://api-higher.gs.com/gateway/api/v1/graphql",
                method="POST",
                headers=self.headers_job,
                body=self.payload_job(source_id),
                callback=self.parse_job,
                meta={"job": job_dict, 'sops_keep_headers': True, "is_internship": True},
            )

        if (page_no + 1) * self.page_size < self.total_count_internships:
            page_no += 1
            internship_payload = self.payload_internships(page_no)
            yield scrapy.Request(
                url="https://api-higher.gs.com/gateway/api/v1/graphql",
                method="POST",
                headers=self.headers_results,
                body=internship_payload,
                callback=self.parse_internship,
                meta={"page_no": page_no, 'sops_keep_headers': True},
                errback=self.handle_error,
            )
        else:
            self.logger.info(f"✅ All internships have been viewed for this round of {self.total_count_internships} internships.")

    def parse_job(self, response):
        job = response.meta["job"]
        is_internship = response.meta.get("is_internship", False)
        data = json.loads(response.text).get("data", {})
        # print(f"🤖🤖🤖 Data for {job['url']}: {data}")
        if data:
            data_role = data.get("role", {})
            if data_role is None:
                self.logger.warning(f"❌ No data['role'] found for job: {job['url']}")
                return
            if data_role.get("locations", None) is not None:
                locations = [loc["city"] for loc in data_role["locations"]]
            else:
                locations = []
            min_salary = None
            max_salary = None
            currency = None
            job_compensation_text = ""
            description = ""
            if data_role:
                compensation = data_role.get("compensation", {})

                if compensation:
                    min_salary = compensation.get("minSalary", "N/A")
                    max_salary = compensation.get("maxSalary", "N/A")
                    currency = compensation.get("currency", "N/A")
                    job_compensation_text = f"Min salary:\n {min_salary}\nMax salary:\n {max_salary}\nCurrency:\n {currency}\n"

                description = data_role.get("descriptionHtml")
                description = markdownify(description)

                job_posting_text = f"""Job title:\n {data_role.get('jobTitle', 'N/A')}
                        Is this job listed in the internships section: \n {is_internship}
                        Corporate title:\n {data_role.get('corporateTitle', 'N/A')}
                        Job function:\n {data_role.get('jobFunction', 'N/A')}
                        Division:\n {data_role.get('division', 'N/A')}
                        Skills:\n {data_role.get('skillset', 'N/A')}
                        {job_compensation_text}
                        Description:\n
                        {description}

                """
                job_info = get_job_info(job_posting_text)

                further_info = {
                    "id_unique": job["id_unique"],
                    "title": data_role.get("jobTitle", "N/A"),
                    "url": job["url"],
                    "description": description,
                    "location": locations,
                    "role_id": data_role.get("roleId", "N/A"),
                    "corporate_title": data_role.get("corporateTitle", "N/A"),
                    "job_function": data_role.get("jobFunction", "N/A"),
                    "division": data_role.get("division", "N/A"),
                    "skillset": data_role.get("skillset", "N/A"),
                    "min_salary": min_salary,
                    "max_salary": max_salary,
                    "currency": currency,
                    "apply_active": data_role.get("applyActive"),
                    "job_type": data_role.get("jobType"),
                    "source_id": data_role.get("externalSource", {}).get(
                        "sourceId", "N/A"
                    ),
                    "external_application_url": data_role.get("externalSource", {}).get(
                        "externalApplicationUrl", "N/A"
                    ),
                    "apply_in_external_source": data_role.get("externalSource", {}).get(
                        "applyInExternalSource"
                    ),
                    "secondary_source_id": data_role.get("externalSource", {}).get(
                        "secondarySourceId", "N/A"
                    ),
                    "status": job["status"],
                }

                yield {**further_info, **job_info}
            else:
                self.logger.error(f"❌ No data found for job: {job['url']}")

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(GoldmanSachsSpider)
    process.start()
