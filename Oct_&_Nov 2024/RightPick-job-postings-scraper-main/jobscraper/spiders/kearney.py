import json
import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from collections import defaultdict
import os
from jobscraper.spiders import close_spider

MAX_JOBS = 1_000_000


class KearneySpider(scrapy.Spider):
    name = "kearney"
    start_url = "https://kearney.recsolu.com/job_boards/EPd41qlA4_03IncZMnWyRQ/search?query=&filters=12163&page_number={page}&job_board_tab_identifier=95fa05b7-edad-427c-a45a-1fa522082f8c"
    log_dir = f'data/logs/{name}'
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        'LOG_FILE': f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        'LOG_LEVEL': 'DEBUG',
        'LOG_ENABLED': True,
    }
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, 'max_jobs', MAX_JOBS))
        yield scrapy.Request(url=self.start_url.format(page=1), meta={'page': 1})

    def parse(self, response):
        page_html = scrapy.Selector(text=json.loads(response.text).get('html'))
        jobs = page_html.css('a.search-results__req_title')
        total_jobs = len(jobs)
        self.remove_obsolete_jobs = self.max_jobs >= total_jobs

        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f'🛑 Reached max jobs: {self.max_jobs}. Stopping...')
                return
            job_dict = {
                'title': job.css('::text').get('').strip(),
                'url': response.urljoin(job.css('::attr(href)').get()),
            }
            job_dict = defaultdict(lambda: None, job_dict)
            id_unique = get_id_unique(self.name, job_dict)
            job_dict['id_unique'] = id_unique
            self.scraped_jobs_dict[id_unique] = job_dict['title']
            self.seen_jobs_count += 1
            if id_unique in self.seen_jobs_set:
                self.logger.info(f'👀 Job "{job_dict["title"]}" already seen. Skipping...')
                continue
            yield response.follow(url=job_dict['url'], callback=self.parse_job, meta={'job_dict': job_dict})

        if self.fetched_count < self.max_jobs and json.loads(response.text).get('html') != "No search results found":
            page = response.meta['page'] + 1
            yield scrapy.Request(url=self.start_url.format(page=page), meta={'page': page})
        else:
            self.logger.info(f'🛑 No more jobs found or reached max jobs: {self.max_jobs}. Stopping...')
            return

    def parse_job(self, response):
        job_dict = response.meta['job_dict']
        cities_list = []
        cities = response.xpath(
            '//div[contains(@class,"details-top__title")]/h1/following-sibling::span[position()>1]//text()').getall()
        for city in cities:
            if '&' in city:
                cities_list.extend(city.split('&'))
            elif ',' in city:
                cities_list.extend(city.split(','))
            else:
                cities_list.append(city.strip())
        cleaned_cities = [x.strip() for x in cities_list]
        further_info = {
            'location': cleaned_cities,
            'apply_link': scrapy.Selector(text=response.css('script#jobs-redirect-maodl::text').get('')).css(
                'a.btn-apply::attr(href)').get(''),
            'description': markdownify(response.css('section.job-details__description').get()),
            'job_type': response.css('div.details-top__title>h1+span::text').get().strip()
        }
        job_posting_text = f"""Job title:\n {job_dict['title']}
                        Description:\n{further_info['description']}
                        """
        job_info = get_job_info(job_posting_text)
        further_info = defaultdict(lambda: None, {**job_dict, **further_info})
        self.fetched_count += 1
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(KearneySpider)
    process.start()