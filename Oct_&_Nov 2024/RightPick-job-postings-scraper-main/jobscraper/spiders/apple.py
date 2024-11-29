import json
import os
from collections import defaultdict
from datetime import datetime
from math import ceil
import scrapy
from markdownify import markdownify
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from jobscraper.spiders import close_spider


class AppleSpider(scrapy.Spider):
    name = "apple"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    }
    url = (
        "https://jobs.apple.com/en-us/search?team=business-intelligence-and-analytics-OPMFG-BIA%20business-proce"
        "ss-management-OPMFG-BPM%20supply-demand-management-and-npi-readiness-OPMFG-SDMNR%20retail-and-e-commerc"
        "e-fulfillment-OPMFG-RECF%20logistics-and-supply-chain-OPMFG-SCL%20sales-planning-and-operations-OPMFG-S"
        "PO%20procurement-OPMFG-PRC%20manufacturing-and-operations-engineering-OPMFG-MFGE%20quality-engineering-"
        "OPMFG-QE%20supplier-responsibility-OPMFG-SR%20program-management-OPMFG-PRMGMT%20acoustic-technologies-H"
        "RDWR-ACT%20analog-and-digital-design-HRDWR-ADD%20architecture-HRDWR-ARCH%20battery-engineering-HRDWR-BE"
        "%20camera-technologies-HRDWR-CAM%20display-technologies-HRDWR-DISP%20engineering-project-management-HRD"
        "WR-EPM%20environmental-technologies-HRDWR-ENVT%20health-technology-HRDWR-HT%20machine-learning-and-ai-H"
        "RDWR-MCHLN%20mechanical-engineering-HRDWR-ME%20process-engineering-HRDWR-PE%20reliability-engineering-H"
        "RDWR-REL%20sensor-technologies-HRDWR-SENT%20silicon-technologies-HRDWR-SILT%20system-design-and-test-en"
        "gineering-HRDWR-SDE%20wireless-hardware-HRDWR-WT%20business-development-SLDEV-BUSDEV%20account-manageme"
        "nt-SLDEV-CC%20sales-planning-and-operations-SLDEV-SO%20field-and-solutions-engineering-SLDEV-FSE%20apps"
        "-and-frameworks-SFTWR-AF%20cloud-and-infrastructure-SFTWR-CLD%20core-operating-systems-SFTWR-COS%20devo"
        "ps-and-site-reliability-SFTWR-DSR%20engineering-project-management-SFTWR-EPM%20information-systems-and-"
        "technology-SFTWR-ISTECH%20machine-learning-and-ai-SFTWR-MCHLN%20security-and-privacy-SFTWR-SEC%20softwa"
        "re-quality-automation-and-tools-SFTWR-SQAT%20wireless-software-SFTWR-WSFT%20machine-learning-infrastruc"
        "ture-MLAI-MLI%20deep-learning-and-reinforcement-learning-MLAI-DLRL%20natural-language-processing-and-sp"
        "eech-technologies-MLAI-NLP%20computer-vision-MLAI-CV%20applied-research-MLAI-AR%20services-marketing-MK"
        "TG-SVCM%20product-marketing-MKTG-PM%20marketing-communications-MKTG-MKTCM%20corporate-communications-MK"
        "TG-CRPCM%20internships-STDNT-INTRN%20corporate-STDNT-CORP%20apple-support-college-program-STDNT-ACCP%20"
        "apple-campus-leader-STDNT-ACR%20industrial-design-DESGN-ID%20human-interface-design-DESGN-HID%20communi"
        "cations-design-DESGN-CMD&page={}"
    )
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    run_completed = False

    def start_requests(self):
        yield scrapy.Request(self.url.format(1), headers=self.headers)

    def parse(self, response, **kwargs):
        data = json.loads(
            response.xpath("//script/text()").re_first("window.APP_STATE = (.*);")
        )
        total_records = data.get("totalRecords", 0)
        self.max_jobs = getattr(self, "max_jobs", total_records)
        self.max_jobs = int(self.max_jobs)
        self.remove_obsolete_jobs = self.max_jobs >= total_records
        seen_jobs_set = get_seen_jobs(self.name)
        for job in data.get("searchResults", []):
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            detail_url = "https://jobs.apple.com/en-us/details/{}/{}?team={}".format(
                job.get("positionId"),
                job.get("transformedPostingTitle"),
                job.get("team", {}).get("teamCode", ""),
            )
            job_dict = {
                "title": job.get("postingTitle", ""),
                "id": job.get("positionId"),
                "url": detail_url,
            }
            id_unique = get_id_unique(self.name, job_dict)
            job_dict = defaultdict(lambda: None, job_dict)
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1
            job_dict["id_unique"] = id_unique
            yield scrapy.Request(
                url=detail_url,
                headers=self.headers,
                callback=self.detail,
                meta={"item": job_dict},
            )
        page = data.get("page")
        total_pages = ceil(total_records / 20)
        if page < total_pages and self.fetched_count < self.max_jobs:
            page += 1
            yield scrapy.Request(self.url.format(page), headers=self.headers)
        else:
            self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")

    def detail(self, response):
        data = json.loads(
            response.xpath("//script/text()").re_first("window.APP_STATE = (.*);")
        ).get("jobDetails", {})
        item = response.meta.get("item")

        # Extract location information
        locations_entry = data.get("locations", [])
        extracted_locations = []
        for loc in locations_entry:
            if "city" in loc:
                location_name = loc["city"]
            elif "name" in loc:
                location_name = loc["name"]
            elif "countryName" in loc:
                location_name = loc["countryName"]
            extracted_locations.append(location_name)
        
        # Combine job summary and description
        job_summary = data.get("jobSummary", "")
        description_short = data.get("description", "")
        if job_summary:
            full_description = job_summary + "\n\n" + description_short
        else:
            full_description = description_short

        further_info = {
            "id_unique": item["id_unique"],
            "title": item["title"],
            "url": item["url"],
            "description": markdownify(full_description),
            "description_short": markdownify(description_short),
            "location": extracted_locations,
            "job_summary": markdownify(data.get("jobSummary", "")),
            "key_qualifications": markdownify(data.get("keyQualifications", "")),
            "education_and_experience": markdownify(
                data.get("educationAndExperience", "")
            ),
            "additional_requirements": markdownify(
                data.get("additionalRequirements", "")
            ),
            "pay_and_benefits": markdownify(
                "\n".join(
                    [
                        i["content"]
                        for i in data.get("postingSupplements", {}).get("footer", [])
                        if i.get("label") == "Pay & Benefits"
                    ]
                )
            ),
            "posted_date": data.get("postingDate", ""),
            "standard_weekly_hours": data.get("standardWeeklyHours", ""),
            "team_names": data.get("teamNames"),
            "employment_type": data.get("employmentType"),
        }
        job_posting_text = f"""Job title:\n {further_info['title']}
                        Key Qualifications:\n{further_info['key_qualifications']}
                        Description:\n{further_info['description']}
                        Education and Experience:\n{further_info['education_and_experience']}
                        Additional Requirements:\n{further_info['additional_requirements']}
                        Pay and Benefits:\n{further_info['pay_and_benefits']}
                        """
        job_info = get_job_info(job_posting_text)
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(AppleSpider)
    process.start()
