import html
import json
import scrapy
from parsel import Selector
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from collections import defaultdict
import os
from jobscraper.spiders import close_spider

MAX_JOBS = 1_000_000


class CitigroupSpider(scrapy.Spider):
    name = "citigroup"
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
    run_completed = False

    start_urls = [
        "https://jobs.citi.com/search-jobs/results?ActiveFacetID=19628&CurrentPage=1&RecordsPerPage=10000&Distance=50&RadiusUnitType=0&Keywords=&Location=&ShowRadius=False&IsPagination=False&CustomFacetName=&FacetTerm=&FacetType=0&FacetFilters%5B0%5D.ID=68328&FacetFilters%5B0%5D.FacetType=1&FacetFilters%5B0%5D.Count=3&FacetFilters%5B0%5D.Display=BCMA+Capital+Markets&FacetFilters%5B0%5D.IsApplied=true&FacetFilters%5B0%5D.FieldName=&FacetFilters%5B1%5D.ID=68095&FacetFilters%5B1%5D.FacetType=1&FacetFilters%5B1%5D.Count=1&FacetFilters%5B1%5D.Display=BCMA+Corporate+Banking&FacetFilters%5B1%5D.IsApplied=true&FacetFilters%5B1%5D.FieldName=&FacetFilters%5B2%5D.ID=67860&FacetFilters%5B2%5D.FacetType=1&FacetFilters%5B2%5D.Count=11&FacetFilters%5B2%5D.Display=BCMA+Investment+Banking&FacetFilters%5B2%5D.IsApplied=true&FacetFilters%5B2%5D.FieldName=&FacetFilters%5B3%5D.ID=8295216&FacetFilters%5B3%5D.FacetType=1&FacetFilters%5B3%5D.Count=172&FacetFilters%5B3%5D.Display=Data+Governance&FacetFilters%5B3%5D.IsApplied=true&FacetFilters%5B3%5D.FieldName=&FacetFilters%5B4%5D.ID=8295232&FacetFilters%5B4%5D.FacetType=1&FacetFilters%5B4%5D.Count=14&FacetFilters%5B4%5D.Display=Data+Science&FacetFilters%5B4%5D.IsApplied=true&FacetFilters%5B4%5D.FieldName=&FacetFilters%5B5%5D.ID=19840&FacetFilters%5B5%5D.FacetType=1&FacetFilters%5B5%5D.Count=38&FacetFilters%5B5%5D.Display=Equities&FacetFilters%5B5%5D.IsApplied=true&FacetFilters%5B5%5D.FieldName=&FacetFilters%5B6%5D.ID=19844&FacetFilters%5B6%5D.FacetType=1&FacetFilters%5B6%5D.Count=1&FacetFilters%5B6%5D.Display=Global+Transaction+Services&FacetFilters%5B6%5D.IsApplied=true&FacetFilters%5B6%5D.FieldName=&FacetFilters%5B7%5D.ID=8165328&FacetFilters%5B7%5D.FacetType=1&FacetFilters%5B7%5D.Count=3&FacetFilters%5B7%5D.Display=Innovation+Lab&FacetFilters%5B7%5D.IsApplied=true&FacetFilters%5B7%5D.FieldName=&FacetFilters%5B8%5D.ID=19636&FacetFilters%5B8%5D.FacetType=1&FacetFilters%5B8%5D.Count=54&FacetFilters%5B8%5D.Display=Institutional+Sales&FacetFilters%5B8%5D.IsApplied=true&FacetFilters%5B8%5D.FieldName=&FacetFilters%5B9%5D.ID=70308&FacetFilters%5B9%5D.FacetType=1&FacetFilters%5B9%5D.Count=53&FacetFilters%5B9%5D.Display=Institutional+Trading&FacetFilters%5B9%5D.IsApplied=true&FacetFilters%5B9%5D.FieldName=&FacetFilters%5B10%5D.ID=19848&FacetFilters%5B10%5D.FacetType=1&FacetFilters%5B10%5D.Count=2&FacetFilters%5B10%5D.Display=Investment+Banking&FacetFilters%5B10%5D.IsApplied=true&FacetFilters%5B10%5D.FieldName=&FacetFilters%5B11%5D.ID=81234&FacetFilters%5B11%5D.FacetType=1&FacetFilters%5B11%5D.Count=336&FacetFilters%5B11%5D.Display=Operations+%26+Technology&FacetFilters%5B11%5D.IsApplied=true&FacetFilters%5B11%5D.FieldName=&FacetFilters%5B12%5D.ID=70310&FacetFilters%5B12%5D.FacetType=1&FacetFilters%5B12%5D.Count=98&FacetFilters%5B12%5D.Display=Product+Management+and+Development&FacetFilters%5B12%5D.IsApplied=true&FacetFilters%5B12%5D.FieldName=&FacetFilters%5B13%5D.ID=19863&FacetFilters%5B13%5D.FacetType=1&FacetFilters%5B13%5D.Count=35&FacetFilters%5B13%5D.Display=Programming&FacetFilters%5B13%5D.IsApplied=true&FacetFilters%5B13%5D.FieldName=&FacetFilters%5B14%5D.ID=19865&FacetFilters%5B14%5D.FacetType=1&FacetFilters%5B14%5D.Count=33&FacetFilters%5B14%5D.Display=Quantitative+Analysis&FacetFilters%5B14%5D.IsApplied=true&FacetFilters%5B14%5D.FieldName=&FacetFilters%5B15%5D.ID=19623&FacetFilters%5B15%5D.FacetType=1&FacetFilters%5B15%5D.Count=26&FacetFilters%5B15%5D.Display=Research&FacetFilters%5B15%5D.IsApplied=true&FacetFilters%5B15%5D.FieldName=&FacetFilters%5B16%5D.ID=19628&FacetFilters%5B16%5D.FacetType=1&FacetFilters%5B16%5D.Count=24&FacetFilters%5B16%5D.Display=Trading&FacetFilters%5B16%5D.IsApplied=true&FacetFilters%5B16%5D.FieldName=&SearchResultsModuleName=SearchResults+-+Technology&SearchFiltersModuleName=Search+Filters&SortCriteria=0&SortDirection=0&SearchType=5&PostalCode=&ResultsType=0&fc=&fl=&fcf=&afc=&afl=&afcf="]

    def parse(self, response):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        self.remove_obsolete_jobs = self.max_jobs >= MAX_JOBS

        json_data = json.loads(response.text)

        # Step 2: Extract the HTML string from the 'filters' field
        html_content = json_data['results']

        # Step 3: Parse the HTML string using parsel.Selector
        selector = Selector(text=html_content)
        job_urls = selector.css('.job-location+a::attr(href)').getall()
        ids = selector.css('.job-location+a::attr(data-job-id)').getall()
        titles = selector.css('.job-location+a h2::text').getall()
        domain = 'https://jobs.citi.com'

        for url, id, title in zip(job_urls, ids, titles):
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return

            url = domain + url
            job_dict = {
                "id": id,
                "url": url,
                "title": title,
            }

            job_dict = defaultdict(lambda: None, job_dict)
            id_unique = get_id_unique(
                self.name, job_dict
            )
            job_dict["id_unique"] = id_unique
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1

            if id_unique in self.seen_jobs_set:
                self.logger.info(f'👀 Job "{job_dict["title"]}" already seen. Skipping...')
                continue

            self.fetched_count += 1
            yield scrapy.Request(url, callback=self.job_detail, meta={'job_dict': job_dict})

    def job_detail(self, response):
        data = json.loads(response.css('[type="application/ld+json"]::text').get())
        job_dict = response.meta["job_dict"]
        id_unique = job_dict.get('id_unique')
        id = job_dict.get('id')
        url = job_dict.get('url')
        title = data.get('title')
        job_locations = data.get('jobLocation', [])
        full_addresses = [
            f"{loc['address']['addressLocality']} {loc['address']['addressRegion']} {loc['address']['addressCountry']}"
            for loc in job_locations
        ]
        datePosted = data.get('datePosted')
        description = markdownify(data.get('description', ''))
        apply_link = response.css('.job-apply::attr(href)').get()
        job_type = data.get('employmentType')
        job_posting_text = f"""Job title:\n {title}
                                Description:\n{description}
                                """
        further_info = {
            'id_unique': id_unique,
            'url': url,
            'title': title,
            'location': full_addresses,
            'description': description,
            'id': id,
            'apply_link': apply_link,
            'date_posted': datePosted,
            'job_type': job_type,
        }
        job_info = get_job_info(job_posting_text)
        yield {**further_info, **job_info}


    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(CitigroupSpider)
    process.start()
