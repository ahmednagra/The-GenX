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


class PinterestSpider(scrapy.Spider):
    name = "pinterest"
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
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6",
            "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        }
        url = "https://www.pinterestcareers.com/jobs/?search=&team=Corporate+Strategy&team=Engineering&team=Marketing+%26+Communications&team=Product&team=Sales&pagesize=20#results"
        yield scrapy.Request(
            url,
            callback=self.parse,
            headers=headers,
            meta={"sops_keep_headers": True, "sops_residential": True},
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
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6",
            "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        }
        ASLBSA = "000396ec0631e5a52b000789740bdfe35cc98289cc23fa6f8893837a316baa63834f"
        ASLBSACORS = (
            "000396ec0631e5a52b000789740bdfe35cc98289cc23fa6f8893837a316baa63834f"
        )
        try:
            response_cookies = response.headers.getlist("Set-Cookie")
            cookies_str = b"".join(response_cookies).decode("utf-8")
            aslbsa_value = cookies_str.split("ASLBSA=")[1].split(";")[0]
            aslbsacors_value = cookies_str.split("ASLBSACORS=")[1].split(";")[0]
        except:
            aslbsa_value = ""
            aslbsacors_value = ""
        cookies = {
            "ASLBSA": aslbsa_value or ASLBSA,
            "ASLBSACORS": aslbsacors_value or ASLBSACORS,
            "OptanonAlertBoxClosed": datetime.utcnow().isoformat(
                timespec="milliseconds"
            )
            + "Z",
            # 'eupubconsent-v2': 'CQF7olgQF7olgAcABBENBJFsAP_gAAAAACiQKftV_G__bWlr8X73aftkeY1P9_h77sQxBhfJE-4FzLvW_JwXx2ExNA36tqIKmRIAu3bBIQNlGJDUTVCgaogVryDMaE2coTNKJ6BkiFMRM2dYCF5vm4tj-QKY5vr991dx2B-t7dr83dzyz4VHn3a5_2a0WJCdA5-tDfv9bROb-9IOd_x8v4v8_F_rE2_eT1l_tWvp7D9-cts7_XW89_fff_9Ln_-uB_-_3_gAAAAA.f_wAAAAAAAAA',
            # 'OptanonConsent': 'isGpcEnabled=0&datestamp=Thu+Oct+03+2024+08%3A28%3A55+GMT-0700+(Pacific+Daylight+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0002%3A1%2CC0001%3A1%2CV2STACK42%3A1&geolocation=%3B&AwaitingReconsent=false',
        }
        links = response.css(".js-view-job::attr(href)").getall()
        ids = response.css(".card-title + div::attr(data-id)").getall()
        titles = response.css(".js-view-job::text").getall()
        for link, id, title in zip(links, ids, titles):
            url = "https://www.pinterestcareers.com" + link
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
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
            yield scrapy.Request(
                url,
                callback=self.detail_page,
                meta={"id": id, "sops_keep_headers": True, "sops_residential": True},
                headers=headers,
                cookies=cookies,
                errback=self.handle_error,
            )
        next_page = response.css('.page-link[aria-label="Next page"]::attr(href)').get(
            ""
        )
        if next_page:
            yield scrapy.Request(
                next_page,
                headers=headers,
                cookies=cookies,
                callback=self.parse,
                meta={"sops_keep_headers": True, "sops_residential": True},
                errback=self.handle_error,
            )

    def detail_page(self, response):
        id = response.meta.get("id")
        data = response.css('[type="application/ld+json"]::text').get()
        data = json.loads(data)
        title = data.get("title")
        description = data.get("description")
        description = markdownify(description)
        applicant_loc_req = data.get("applicantLocationRequirements", {}).get("name")
        datePosted = data.get("datePosted")
        hiringOrganization = data.get("hiringOrganization", {}).get("name")
        industry = data.get("industry")
        location = []
        job_locations = data.get("jobLocation")
        if isinstance(job_locations, list):
            for loc in job_locations:
                city = loc.get("address", {}).get("addressLocality")
                location.append(city)
        elif isinstance(job_locations, dict):
            city = job_locations.get("address", {}).get("addressLocality")
            location.append(city)
        further_info = {
            "id": id,
            "title": title,
            "description": description,
            "applicant_location_req": applicant_loc_req,
            "date_posted": datePosted,
            "hiring_organization": hiringOrganization,
            "industry": industry,
            "location": location,
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
    process.crawl(PinterestSpider)
    process.start()
