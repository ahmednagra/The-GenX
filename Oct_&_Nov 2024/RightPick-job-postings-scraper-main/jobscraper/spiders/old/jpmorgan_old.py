import json
import os
from collections import defaultdict
from datetime import datetime
import scrapy
from markdownify import markdownify
from scrapy import Selector
from scrapy.crawler import CrawlerProcess
from scrapy.spidermiddlewares.httperror import HttpError
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from jobscraper.spiders import close_spider


class JPMorganSpider(scrapy.Spider):
    name = "jpmorgan_old"
    start_urls = ["http://jpmc.fa.oraclecloud.com/"]
    page_size = 25  # max allowed by the API
    page_offset = 0
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
    url = "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.secondaryLocations,flexFieldsFacet.values&finder=findReqs;siteNumber=CX_1001,facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,limit=25,lastSelectedFacet=CATEGORIES,selectedCategoriesFacet=300000086152753%3B300000086153065%3B300000086153391%3B300000086152593%3B300000086251601%3B300000086251864%3B300000086249543%3B300000086152757%3B300000086152508%3B300000086144020%2C,sortBy=POSTING_DATES_DESC,offset={}"
    headers = {
        "Accept": "*/*",
        "Accept-Language": "en",
        "Content-Type": "application/vnd.oracle.adf.resourceitem+json;charset=utf-8",
        "Ora-Irc-Language": "en",
    }

    def start_requests(self):
        try:
            yield scrapy.Request(
                self.url.format(self.page_offset),
                headers=self.headers,
                callback=self.parse_results,
                meta=dict(
                    sops_keep_headers=True,
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
        seen_jobs_set = get_seen_jobs(self.name)
        data = response.json()
        if "html" in data:
            # Parse the stringified JSON
            data = json.loads(data["html"])
        if "items" not in data:
            self.logger.error(
                f"❌ Error: 'items' field not found in API response: {data}"
            )
            return
        if self.total_count is None:
            self.total_count = int(data.get("items")[0]["TotalJobsCount"])
        self.remove_obsolete_jobs = (
            int(getattr(self, "max_jobs", self.total_count)) >= self.total_count
        )
        max_jobs_or_none = getattr(self, "max_jobs", None)
        job_search_data = data["items"][0].get("requisitionList")
        for job in job_search_data:
            if max_jobs_or_none is not None and self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            job = defaultdict(lambda: None, job)
            id_unique = get_id_unique(self.name, job, title=job["Title"], id=job["Id"])
            self.scraped_jobs_dict[id_unique] = job["Title"]
            self.seen_jobs_count += 1
            if id_unique in seen_jobs_set:
                self.logger.info(f'👀 Job "{job["Title"]}" already seen. Skipping...')
                continue
            self.fetched_count += 1
            job["id_unique"] = id_unique

            url = f"https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails?expand=all&onlyData=true&finder=ById;Id=%22{job.get('Id')}%22,siteNumber=CX_1001"
            yield scrapy.Request(
                url,
                headers=self.headers,
                callback=self.parse_job,
                meta=dict(
                    sops_keep_headers=True,
                    job=job,
                ),
            )
        if self.seen_jobs_count < self.total_count and (
            max_jobs_or_none is None or self.fetched_count < self.max_jobs
        ):
            self.page_offset = self.page_offset + 25
            self.page_number = self.page_number + 1
            self.logger.info(f"🕵️‍♂️ Fetching page {self.page_number}...")
            yield scrapy.Request(
                self.url.format(self.page_offset),
                headers=self.headers,
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
        json_data = response.json()
        data = json_data.get("items")[0]
        if "html" in data:
            # Parse the stringified JSON
            data = json.loads(data["html"])
        else:
            self.logger.warning(f"❌ 'html' field not found in API response: {data}")
        selector = Selector(text=data.get("ExternalDescriptionStr"))
        job_description = " ".join(selector.xpath("//text()").getall()).strip()
        about_us = data.get("CorporateDescriptionStr")
        about_team = data.get("OrganizationDescriptionStr")
        final_job_description = markdownify(job_description + about_us + about_team)
        job_responsibilities = (
            "\n".join(
                [
                    i.strip()
                    for i in selector.xpath(
                        '//strong[contains(text(),"Job Responsibilities")]/../following-sibling::div/ul[1]/li//text()'
                    ).getall()
                    if i.strip()
                ]
            )
            or "\n".join(
                [
                    i.strip()
                    for i in selector.xpath(
                        '//strong[contains(text(),"Job Responsibilities")]/../following-sibling::ul[1]/li//text()'
                    ).getall()
                    if i.strip()
                ]
            )
            or "\n".join(
                [
                    i.strip()
                    for i in selector.xpath(
                        '//strong[contains(text(),"Job responsibilities")]/../following-sibling::div/ul[1]/li//text()'
                    ).getall()
                    if i.strip()
                ]
            )
            or "\n".join(
                [
                    i.strip()
                    for i in selector.xpath(
                        '//strong[contains(text(),"Job responsibilities")]/../following-sibling::ul[1]/li//text()'
                    ).getall()
                    if i.strip()
                ]
            )
        )
        required_qualifications = "\n".join(
            [
                i.strip()
                for i in selector.xpath(
                    '//strong[contains(text(),"Required")]/../following-sibling::div/ul[1]/li//text()'
                ).getall()
                if i.strip()
            ]
        ) or "\n".join(
            [
                i.strip()
                for i in selector.xpath(
                    '//strong[contains(text(),"Required")]/../following-sibling::ul[1]/li//text()'
                ).getall()
                if i.strip()
            ]
        )
        preferred_qualifications = "\n".join(
            [
                i.strip()
                for i in selector.xpath(
                    '//strong[contains(text(),"Preferred")]/../following-sibling::div/ul[1]/li//text()'
                ).getall()
                if i.strip()
            ]
        ) or "\n".join(
            [
                i.strip()
                for i in selector.xpath(
                    '//strong[contains(text(),"Preferred")]/../following-sibling::ul[1]/li//text()'
                ).getall()
                if i.strip()
            ]
        )

        requisition_salary = (
            data.get("requisitionFlexFields")[0]
            if data.get("requisitionFlexFields")
            else {}
        )
        job_posting_text = f"""Job title:\n {data.get('Title')}
                Job function:\n {data.get('JobFunction')}
                Skills:\n {required_qualifications}{preferred_qualifications}
                Description:\n{final_job_description}
                Short Description:\n{job.get('ShortDescriptionStr')}
                Organization Description:\n{data.get('OrganizationDescriptionStr')}
                External Description:\n{data.get('ExternalDescriptionStr')}
                Corporate Description:\n{data.get('CorporateDescriptionStr')}
                Business Unit:\n{data.get('BusinessUnit')}
                Department:\n{data.get('Department')}
                Salary:\n{requisition_salary.get('Value')}
                """
        job_info = get_job_info(job_posting_text)
        cities = []
        work_loc = data.get("workLocation", [])
        other_work_loc = data.get("otherWorkLocations", [])
        for loc in work_loc + other_work_loc:
            cities.append(loc.get("TownOrCity").strip())
        further_info = {
            "company": self.name,
            "id_unique": job["id_unique"],
            "title": data.get("Title"),
            "url": f"https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/job/{data.get('Id')}",
            "description": final_job_description,
            "location": cities,
            "job_identification": data.get("Id"),
            "business_unit": data.get("BusinessUnit"),
            "job_shift": data.get("JobShift"),
            "job_function": data.get("JobFunction"),
            "responsibilities": job_responsibilities,
            "requirements": required_qualifications,
            "preferred_requirements": preferred_qualifications,
            "posting_date": self.date_formating(data.get("ExternalPostedStartDate")),
            "apply_before": self.date_formating(data.get("ExternalPostedEndDate")),
            "short_description": data.get("ShortDescriptionStr"),
            "salary": requisition_salary.get("Value"),
            "job_category": data.get("Category"),
            "job_schedule": data.get("JobSchedule"),
        }
        yield {**further_info, **job_info}

    def date_formating(self, date):
        try:
            dt_object = datetime.fromisoformat(date[:-6])
            formatted_date = dt_object.strftime("%m/%d/%Y, %I:%M %p")
            return formatted_date
        except:
            return date

    def close(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(JPMorganSpider)
    process.start()
