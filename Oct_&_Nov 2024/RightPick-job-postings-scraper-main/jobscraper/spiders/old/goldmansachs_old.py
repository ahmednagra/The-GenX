import scrapy
from datetime import datetime
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from collections import defaultdict
from scrapy.spidermiddlewares.httperror import HttpError
import json
import urllib
from scrapy.crawler import CrawlerProcess
import os
from jobscraper.spiders import close_spider


class GoldmanSachsSpider(scrapy.Spider):
    name = "goldmansachs_old"
    start_urls = ["https://api-higher.gs.com/gateway/api/v1/graphql"]
    page_size = 250  # max allowed by the API
    page_number = 0
    total_count = None
    fetched_count = 0
    seen_jobs_count = 0

    scraped_jobs_dict = dict()
    remove_obsolete_jobs = False

    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }

    query = """query GetRoles($searchQueryInput: RoleSearchQueryInput!) {
  roleSearch(searchQueryInput: $searchQueryInput) {
    totalCount
    items {
      roleId
      corporateTitle
      jobTitle
      jobFunction
      locations {
        primary
        state
        country
        city
        __typename
      }
      status
      division
      skills
      bookmarkId
      jobType {
        code
        description
        __typename
      }
      externalSource {
        sourceId
        __typename
      }
      __typename
    }
    __typename
  }
}"""

    # Payload for job page request
    @staticmethod
    def payload_results(page_number, page_size=page_size, query=query):
        return json.dumps(
            {
                "operationName": "GetRoles",
                "variables": {
                    "searchQueryInput": {
                        "page": {"pageSize": page_size, "pageNumber": page_number},
                        "sort": {"sortStrategy": "RELEVANCE", "sortOrder": "DESC"},
                        "filters": [
                            {
                                "filterCategoryType": "JOB_FUNCTION",
                                "filters": [
                                    {
                                        "filter": "Analytics & Reporting",
                                        "subFilters": [],
                                    },
                                    {"filter": "Banking Analysis", "subFilters": []},
                                    {"filter": "Business Unit COO", "subFilters": []},
                                    {
                                        "filter": "Business Unit Leadership",
                                        "subFilters": [],
                                    },
                                    {"filter": "Credit Risk", "subFilters": []},
                                    {"filter": "Data Analytics", "subFilters": []},
                                    {"filter": "Data Governance", "subFilters": []},
                                    {
                                        "filter": "Digital Operations - Management",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Digital Operations - Specialist",
                                        "subFilters": [],
                                    },
                                    {"filter": "Divisional CFO", "subFilters": []},
                                    {
                                        "filter": "Divisional Leadership",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Finance, Structuring & Execution",
                                        "subFilters": [],
                                    },
                                    {"filter": "Fund Management", "subFilters": []},
                                    {
                                        "filter": "Funding & Capital Planning",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Government Relations",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Investing & Portfolio Management - Private",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Investing & Portfolio Management - Public",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Liquidity Management",
                                        "subFilters": [],
                                    },
                                    {"filter": "Liquidity Risk", "subFilters": []},
                                    {
                                        "filter": "Loan Management & Due Diligence",
                                        "subFilters": [],
                                    },
                                    {"filter": "Market Risk", "subFilters": []},
                                    {"filter": "Model Risk", "subFilters": []},
                                    {"filter": "Operational Risk", "subFilters": []},
                                    {"filter": "Product Management", "subFilters": []},
                                    {"filter": "Project Management", "subFilters": []},
                                    {
                                        "filter": "Quantitative Engineering",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Real Estate Asset Management",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Real Estate Portfolio Management",
                                        "subFilters": [],
                                    },
                                    {"filter": "Research Analyst", "subFilters": []},
                                    {"filter": "Risk Governance", "subFilters": []},
                                    {
                                        "filter": "Security Engineering",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Software Engineering",
                                        "subFilters": [],
                                    },
                                    {
                                        "filter": "Strategic Planning and Transformation",
                                        "subFilters": [],
                                    },
                                    {"filter": "Systems Engineering", "subFilters": []},
                                    {"filter": "Technology Audit", "subFilters": []},
                                    {"filter": "Technology Products", "subFilters": []},
                                    {"filter": "Trade Processing", "subFilters": []},
                                    {"filter": "Trading", "subFilters": []},
                                ],
                            }
                        ],
                        "experiences": ["PROFESSIONAL", "EARLY_CAREER"],
                        "searchTerm": "",
                    }
                },
                "query": query,
            }
        )

    headers_results = {
        "Content-Type": "application/json",
    }

    # Headers for job page request
    headers_job = {
        "Content-Type": "application/json",
    }

    # Payload for job page request
    @staticmethod
    def payload_job(id):
        return json.dumps(
            {
                "operationName": "GetRoleById",
                "variables": {
                    "externalSourceId": f"{id}",
                    "externalSourceFetch": False,
                },
                "query": """query GetRoleById($externalSourceId: String!, $externalSourceFetch: Boolean) {
role(
    externalSourceId: $externalSourceId
    externalSourceFetch: $externalSourceFetch
  ) {
    roleId
    corporateTitle
    jobTitle
    jobFunction
    locations {
      primary
      state
      country
      city
      __typename
    }
    division
    descriptionHtml
    bookmarkId
    jobType {
      code
      description
      __typename
    }
    skillset
    compensation {
      minSalary
      maxSalary
      currency
      __typename
    }
    applyActive
    externalSource {
      externalApplicationUrl
      applyInExternalSource
      sourceId
      secondarySourceId
      __typename
    }
    __typename
  }
}""",
            }
        )

    def start_requests(self):
        try:
            # Update payload with the current page_number
            updated_payload = self.payload_results(self.page_number)

            yield scrapy.Request(
                self.start_urls[0],
                method="POST",
                headers=self.headers_results,
                body=updated_payload,
                callback=self.parse_results,
                meta=dict(
                    sops_keep_headers=True,
                    # sops_bypass="generic_level_3",
                    # sops_residential=True,
                    # sops_optimize_request=True,
                ),
                errback=self.handle_error,
            )
        except json.JSONDecodeError as e:
            print(f"⚠️ Invalid JSON: {e}")

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

        data = response.json()
        if "html" in data:
            # Parse the stringified JSON
            data = json.loads(data["html"])
        if "data" not in data:
            self.logger.error(
                f"❌ Error: 'data' field not found in API response: {data}"
            )
            return

        if self.total_count is None:
            self.total_count = int(data["data"]["roleSearch"]["totalCount"])

        seen_jobs_set = get_seen_jobs(self.name)

        self.remove_obsolete_jobs = (
            int(getattr(self, "max_jobs", self.total_count)) >= self.total_count
        )

        max_jobs_or_none = getattr(self, "max_jobs", None)

        job_search_data = data["data"]["roleSearch"]["items"]

        for job in job_search_data:
            if max_jobs_or_none is not None and self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            job = defaultdict(lambda: None, job)
            id_unique = get_id_unique(
                self.name,
                job,
                title=job["jobTitle"],
                id=job["externalSource"]["sourceId"],
            )
            self.scraped_jobs_dict[id_unique] = job["jobTitle"]
            self.seen_jobs_count += 1
            if id_unique in seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{job["jobTitle"]}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1

            job["id_unique"] = id_unique

            url = f"https://higher.gs.com/roles/{job['externalSource']['sourceId']}"
            job["url"] = url
            yield response.follow(
                "https://api-higher.gs.com/gateway/api/v1/graphql",
                method="POST",
                headers=self.headers_job,
                body=self.payload_job(job["externalSource"]["sourceId"]),
                callback=self.parse_job,
                meta=dict(
                    sops_keep_headers=True,
                    job=job,
                ),
            )

        # Check if there are more pages to fetch and we haven't reached the max_jobs limit
        if self.seen_jobs_count < self.total_count and (
            max_jobs_or_none is None or self.fetched_count < self.max_jobs
        ):
            self.page_number += 1
            self.logger.info(f"🕵️‍♂️ Fetching page {self.page_number}...")
            yield scrapy.Request(
                self.start_urls[0],
                method="POST",
                headers=self.headers_results,
                body=self.payload_results(self.page_number),
                callback=self.parse_results,
                meta=dict(
                    sops_keep_headers=True,
                ),
                errback=self.handle_error,
            )
        else:
            self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")

    def parse_job(self, response):
        job = response.meta["job"]
        data = response.json()
        if "html" in data:
            # Parse the stringified JSON
            data = json.loads(data["html"])
        else:
            self.logger.warning(f"❌ 'html' field not found in API response: {data}")

        locations = [loc["city"] for loc in job["locations"]]

        data_role = defaultdict(lambda: None, data["data"]["role"])

        if data_role["compensation"]:
            data_role["compensation"] = defaultdict(
                lambda: None, data_role["compensation"]
            )
            job_compensation_text = f"Min salary:\n {data_role['compensation']['minSalary']}\nMax salary:\n {data_role['compensation']['maxSalary']}\nCurrency:\n {data_role['compensation']['currency']}\n"
        else:
            data_role["compensation"] = defaultdict(lambda: None)
            job_compensation_text = ""

        description = data_role["descriptionHtml"]
        description = markdownify(description)

        job_posting_text = f"""Job title:\n {job['jobTitle']}
Corporate title:\n {job['corporateTitle']}
Job function:\n {job['jobFunction']}
Division:\n {job['division']}
Skills:\n {job['skillset']}
{job_compensation_text}
Description:\n
{description}
"""
        job_info = get_job_info(job_posting_text)

        further_info = {
            "id_unique": job["id_unique"],
            "title": data_role["jobTitle"],
            "url": job["url"],
            "description": description,
            "location": locations,
            "role_id": data_role["roleId"],
            "corporate_title": data_role["corporateTitle"],
            "job_function": data_role["jobFunction"],
            "division": data_role["division"],
            "bookmark_id": data_role["bookmarkId"],
            "skillset": data_role["skillset"],
            "min_salary": data_role["compensation"]["minSalary"],
            "max_salary": data_role["compensation"]["maxSalary"],
            "currency": data_role["compensation"]["currency"],
            "apply_active": data_role["applyActive"],
            "job_type": data_role["jobType"],
            "source_id": data_role["externalSource"]["sourceId"],
            "external_application_url": data_role["externalSource"][
                "externalApplicationUrl"
            ],
            "apply_in_external_source": data_role["externalSource"][
                "applyInExternalSource"
            ],
            "secondary_source_id": data_role["externalSource"]["secondarySourceId"],
            "status": job["status"],
        }

        yield {**further_info, **job_info}

    def close(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(GoldmanSachsSpider)
    process.start()
