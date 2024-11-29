import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from collections import defaultdict
from parsel import Selector
import json
import os
from jobscraper.spiders import close_spider

MAX_JOBS = 1_000_000


class BlackRockSpider(scrapy.Spider):
    name = "blackrock"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
        'URLLENGTH_LIMIT': 5000,
    }

    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    run_completed = False

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        url = "https://careers.blackrock.com/search-jobs"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # result_url = "https://careers.blackrock.com/search-jobs/results?ActiveFacetID=0&CurrentPage=1&RecordsPerPage={}&Distance=50&RadiusUnitType=0&Keywords=&Location=&ShowRadius=False&IsPagination=True&CustomFacetName=&FacetTerm=&FacetType=0&SearchResultsModuleName=Section+3+-+Search+Results&SearchFiltersModuleName=Section+3+-+Search+Filters&SortCriteria=0&SortDirection=0&SearchType=5&PostalCode=&ResultsType=0&fc=&fl=&fcf=&afc=&afl=&afcf="
        result_url = "https://careers.blackrock.com/search-jobs/results?ActiveFacetID=Trading&CurrentPage=1&RecordsPerPage={}&Distance=50&RadiusUnitType=0&Keywords=&Location=&ShowRadius=False&IsPagination=False&CustomFacetName=&FacetTerm=&FacetType=0&FacetFilters%5B0%5D.ID=Alternative+Investment+Management&FacetFilters%5B0%5D.FacetType=5&FacetFilters%5B0%5D.Count=5&FacetFilters%5B0%5D.Display=Alternative+Investment+Management&FacetFilters%5B0%5D.IsApplied=true&FacetFilters%5B0%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B1%5D.ID=Data+Analytics&FacetFilters%5B1%5D.FacetType=5&FacetFilters%5B1%5D.Count=6&FacetFilters%5B1%5D.Display=Data+Analytics&FacetFilters%5B1%5D.IsApplied=true&FacetFilters%5B1%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B2%5D.ID=Data+Modelling&FacetFilters%5B2%5D.FacetType=5&FacetFilters%5B2%5D.Count=1&FacetFilters%5B2%5D.Display=Data+Modelling&FacetFilters%5B2%5D.IsApplied=true&FacetFilters%5B2%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B3%5D.ID=Data+Operations&FacetFilters%5B3%5D.FacetType=5&FacetFilters%5B3%5D.Count=12&FacetFilters%5B3%5D.Display=Data+Operations&FacetFilters%5B3%5D.IsApplied=true&FacetFilters%5B3%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B4%5D.ID=Data+Science&FacetFilters%5B4%5D.FacetType=5&FacetFilters%5B4%5D.Count=3&FacetFilters%5B4%5D.Display=Data+Science&FacetFilters%5B4%5D.IsApplied=true&FacetFilters%5B4%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B5%5D.ID=Insights+%26+Analytics&FacetFilters%5B5%5D.FacetType=5&FacetFilters%5B5%5D.Count=1&FacetFilters%5B5%5D.Display=Insights+%26+Analytics&FacetFilters%5B5%5D.IsApplied=true&FacetFilters%5B5%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B6%5D.ID=Investment+Management&FacetFilters%5B6%5D.FacetType=5&FacetFilters%5B6%5D.Count=1&FacetFilters%5B6%5D.Display=Investment+Management&FacetFilters%5B6%5D.IsApplied=true&FacetFilters%5B6%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B7%5D.ID=Investment+Performance&FacetFilters%5B7%5D.FacetType=5&FacetFilters%5B7%5D.Count=1&FacetFilters%5B7%5D.Display=Investment+Performance&FacetFilters%5B7%5D.IsApplied=true&FacetFilters%5B7%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B8%5D.ID=Investment+Product&FacetFilters%5B8%5D.FacetType=5&FacetFilters%5B8%5D.Count=5&FacetFilters%5B8%5D.Display=Investment+Product&FacetFilters%5B8%5D.IsApplied=true&FacetFilters%5B8%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B9%5D.ID=Investment+Strategy+%26+Research&FacetFilters%5B9%5D.FacetType=5&FacetFilters%5B9%5D.Count=5&FacetFilters%5B9%5D.Display=Investment+Strategy+%26+Research&FacetFilters%5B9%5D.IsApplied=true&FacetFilters%5B9%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B10%5D.ID=Market+Risk+Management&FacetFilters%5B10%5D.FacetType=5&FacetFilters%5B10%5D.Count=4&FacetFilters%5B10%5D.Display=Market+Risk+Management&FacetFilters%5B10%5D.IsApplied=true&FacetFilters%5B10%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B11%5D.ID=Portfolio+Management&FacetFilters%5B11%5D.FacetType=5&FacetFilters%5B11%5D.Count=16&FacetFilters%5B11%5D.Display=Portfolio+Management&FacetFilters%5B11%5D.IsApplied=true&FacetFilters%5B11%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B12%5D.ID=Project+Management&FacetFilters%5B12%5D.FacetType=5&FacetFilters%5B12%5D.Count=8&FacetFilters%5B12%5D.Display=Project+Management&FacetFilters%5B12%5D.IsApplied=true&FacetFilters%5B12%5D.FieldName=custom_fields.SubTeam&FacetFilters%5B13%5D.ID=Software+Engineering&FacetFilters%5B13%5D.FacetType=5&FacetFilters%5B13%5D.Count=73&FacetFilters%5B13%5D.Display=Software+Engineering&FacetFilters%5B13%5D.IsApplied=true&FacetFilters%5B13%5D.FieldName=custom_fields.SubTeam&SearchResultsModuleName=Section+3+-+Search+Results&SearchFiltersModuleName=Section+3+-+Search+Filters&SortCriteria=0&SortDirection=0&SearchType=5&PostalCode=&ResultsType=0&fc=&fl=&fcf=&afc=&afl=&afcf="
        total_result = int(response.css('.section3__search-results-heading::text').get().split()[0])
        yield scrapy.Request(url=result_url.format(total_result), callback=self.parse_next)

    def parse_next(self, response):
        json_response = json.loads(response.body)
        html_content = json_response['results']
        data = Selector(text=html_content)

        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        jobs = data.css('.section3__search-results-a')
        total_jobs = int(data.css('.section3__search-results-heading::text').get().split()[0])
        self.remove_obsolete_jobs = self.max_jobs >= total_jobs

        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            url = response.urljoin(job.css('a::attr(href)').get())
            job_dict = {
                "id": job.css('a::attr(data-job-id)').get(),
                "url": url,
                "title": job.css('.section3__job-title::text').get().strip(),
            }
            job_dict = defaultdict(lambda: None, job_dict)
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
                url=url,
                callback=self.job_detail,
                meta={"job_dict": job_dict},
            )

    def job_detail(self, response):
        further_info = response.meta["job_dict"]
        further_info["category"] = response.css('.job-info-jd .section4__job-info-label+ .section4__job-info-item::text').get()
        further_info["posted_date"] = response.css('.job-date .section4__job-info-item::text').get()
        further_info["job_id"] = response.css('.job-id .section4__job-info-item::text').get()
        further_info["apply_link"] = response.css('.top::attr(href)').get()
        location = response.css('.section4__job-location::text').get().strip()
        
        location_mapping = {
            "Gurgaon, Haryana": "Gurgaon",
            "Paris, Île-de-France Region": "Paris",
            "Budapest, Budapest": "Budapest",
            "Edinburgh, Scotland": "Edinburgh",
            "New York, NY": "New York City",
            "Amsterdam, North Holland": "Amsterdam",
            "London, England": "London",
            "Melbourne, Victoria": "Melbourne",
            "Wilmington, DE": "Wilmington, DE",
            "Mexico City, Mexico City": "Mexico City",
            "Mumbai, Maharashtra": "Mumbai",
            "Atlanta, GA": "Atlanta",
            "San Francisco, CA": "San Francisco",
            "Hong Kong, Central and Western District": "Hong Kong",
            "Santa Monica, CA": "Santa Monica"
        }
        
        further_info["location"] = [location_mapping.get(location, location)]

        further_info["description"] = markdownify(response.css(".ats-description").get())
        job_posting_text = f"""Job title:\n {further_info['title']}
                                        Description:\n{further_info['description']}
                                        """
        job_info = get_job_info(job_posting_text)
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(BlackRockSpider)
    process.start()
