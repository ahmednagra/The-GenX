import scrapy
from datetime import datetime
from dataextraction import get_job_info, get_seen_jobs, get_id_unique 
from markdownify import markdownify
from collections import defaultdict
import os
from jobscraper.spiders import close_spider
MAX_JOBS = 1_000_000

class BainSpider(scrapy.Spider):
    name = "bain_old"
    start_urls = [
        "https://careers.bain.com/jobs/SearchJobs/?4784=%5B21417662%2C21417665%2C21417667%2C21417670%2C21417672%2C21417674%5D&4784_format=4033&listFilterMode=1&folderRecordsPerPage=10&"
    ]

    log_dir = f'data/logs/{name}'
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = {
        'LOG_FILE': f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        'LOG_LEVEL': 'DEBUG',
        'LOG_ENABLED': True,
    }

    scraped_jobs_dict = dict()
    remove_obsolete_jobs = False

    
    def parse(self, response):
        seen_jobs_set = get_seen_jobs(self.name)
        current_jobs_count = 0

        data = response.css("article.article--result")

        self.max_jobs = getattr(self,'max_jobs', MAX_JOBS)
        self.max_jobs = int(self.max_jobs)

        self.remove_obsolete_jobs = self.max_jobs >= MAX_JOBS

        job_count = 0
        job_index = 0
        
        while job_count < self.max_jobs and job_index < len(data):
            job_html = data[job_index]
            job_index += 1

            url = job_html.css("h3.article__header__text__title--4 > a::attr(href)").get()
            title = job_html.css("h3.article__header__text__title--4 > a::text").get().strip()

            job = { "url": url, "title": title }
            job = defaultdict(lambda: None, job)
            id_unique = get_id_unique(self.name, job)
            self.scraped_jobs_dict[id_unique] = job['title']
            if id_unique in seen_jobs_set:
                self.logger.info(f'👀 Job "{job["title"]}" already seen. Skipping...')
                continue

            job_count += 1
            current_jobs_count += 1

            # location = job_html.css("div.article__header__text__subtitle ::text").get().strip() or "N/A"
            apply_url = job_html.css("a.button--secondary::attr(href)").get()

            initial_info = {"url": url, "title": title, "apply_url": apply_url}

            yield response.follow(
                url, 
                callback=self.parse_job, 
                meta=dict(
                    sops_render_js = True,
                    initial_info=initial_info,
                )
            )

        if current_jobs_count < self.max_jobs:
            next_page = response.css("a.paginationNextLink::attr(href)").get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)
            
        # write_seen_jobs(self.name, self.scraped_jobs_dict)
        
    def parse_job(self, response):
        initial_info = response.meta["initial_info"]

        # initial_info['title'] = response.xpath('//div[div[contains(text(),"Job Title")]]/div[@class="article__content__view__field__value"]/text()').get().strip()

        initial_info['id'] = int(response.xpath('//div[div[contains(text(),"Job ID")]]/div[@class="article__content__view__field__value"]/text()').get().strip())

        initial_info['areas_of_work'] = response.xpath('//div[div[contains(text(),"Areas of Work")]]/div[@class="article__content__view__field__value"]/text()').get().strip()

        initial_info['employment_type'] = response.xpath('//div[div[contains(text(),"Employment Type")]]/div[@class="article__content__view__field__value"]/text()').get().strip()

        locations = response.xpath('//div[div[contains(text(),"Location(s)")]]/div[@class="article__content__view__field__value"]/text()').get().strip()
        initial_info['location'] = [loc.strip() for loc in locations.split(",")]


        description = response.css("section.section.js_views").get()
        description = markdownify(description)
        
        job_posting_text = f"""Job title:\n {initial_info["title"]} 
Description:\n
{description}
"""     
        job_info = get_job_info(job_posting_text)
        yield {**initial_info, "description": description, **job_info}

    def close(self, reason):
        close_spider(self, reason)