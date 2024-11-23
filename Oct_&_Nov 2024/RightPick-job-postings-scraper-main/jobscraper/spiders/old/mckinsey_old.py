import scrapy
from datetime import datetime

from scrapy_playwright.page import PageMethod
from dataextraction import get_job_info
# from jobscraper.proxy import get_proxy_url
from jobscraper.itemloaders import JobLoader
from jobscraper.items import JobItem
import os
# from dotenv import load_dotenv, find_dotenv
# load_dotenv(find_dotenv())
# SCRAPEOPS_API_KEY = os.getenv('SCRAPEOPS_API_KEY')
#         "PLAYWRIGHT_LAUNCH_OPTIONS": {
#             "proxy": {
#                 "server": "http://proxy.scrapeops.io:5353",
#                 "username": "scrapeops.render_js=true",
#                 "password": SCRAPEOPS_API_KEY,
#             },
#         }


get_proxy_url = lambda url: url
class MckinseyOldSpider(scrapy.Spider):
    name = 'mckinsey_old'
    start_urls = ['https://www.mckinsey.com/careers/search-jobs']

    log_dir = f'data/logs/{name}'
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = { 
        'SCRAPEOPS_PROXY_SETTINGS': {'render_js': True},
        'LOG_FILE': f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        'LOG_LEVEL': 'DEBUG',
        'LOG_ENABLED': True,
    }

    page_number = 1
    job_count = 0
    jobs = {}
    page_methods = [
        PageMethod('evaluate', '''() => {
            var cookieBtn = document.querySelector('#onetrust-reject-all-handler');
            if(cookieBtn){
                cookieBtn.click();
            }
        }'''),
        # PageMethod('wait_for_timeout', 2000),
        PageMethod('wait_for_selector', 'ul.job-list'),
        PageMethod('wait_for_selector', 'div.city-list-container'),
        PageMethod('evaluate', '''() => {
            const buttons = document.querySelectorAll('div.city-list-container li.show-all');
            buttons.forEach(button => button.click());
        }'''),
        PageMethod('wait_for_timeout', 2000),
        # PageMethod('screenshot', path="screenshot.png", full_page=True),
    ]

    def start_requests(self):
        url = self.start_urls[0] + f'?page={self.page_number}'
        yield scrapy.Request(
            url=get_proxy_url(url), 
            callback=self.parse,
            meta=dict(
            playwright=True,
            playwright_include_page=True,
            playwright_page_methods=self.page_methods,
            errback=self.errback,
        ))

    async def parse(self, response):
        max_jobs = getattr(self,'max_jobs', None)
        if max_jobs is not None:
            max_jobs = int(max_jobs)

        page = response.meta["playwright_page"]
        job_listings = response.css('ul.job-list li.job-listing')

        self.job_count = len(job_listings)

        if self.job_count < self.page_number * 20 or (max_jobs is not None and self.job_count >= max_jobs):
            for job_idx, job in enumerate(job_listings):

                job_loader = JobLoader(item=JobItem(), selector=job)

                title = job.css("h2.headline a::text").get()
                title = title.replace('Job title\n', '')
                job_loader.add_value('title', title)
                
                description = job.css("p.description::text").get()
                description = description.replace('Job description\n', '')
                job_loader.add_value('description', description)

                interest = job.css("p.interests::text").get()
                interest = interest.replace('Job interest\n', '')
                job_loader.add_value('interest', interest)

                url_id = job.css("h2.headline a").attrib['href'].replace('./search-jobs/jobs/', '')
                job_loader.add_value('url_id', url_id)
                
                job_loader.add_value('url', response.urljoin(url_id))

                singleCity = job.css('div.city-list-container div.city::text').get()
                if singleCity:
                    location = [[singleCity.strip()]]
                else:
                    showMore = job.css('div.city-list-container li.show-all').get()
                    assert(not showMore)
                    cities = job.css('div.city-list-container ul.list.list-pipe li.city::text').getall()
                    location = [[c.strip() for c in cities]]

                job_loader.add_value('location', location)

                job_posting_text = f'Job title: {title}\n\nJob interest: {interest}\n\nDescription:\n {description}'

                job_info = get_job_info(job_posting_text)

                if job_info is not None:
                    for key, value in job_info.items():
                        job_loader.add_value(key, value)

                yield job_loader.load_item()

                if max_jobs is not None and job_idx+1 >= max_jobs:
                    break

            await page.close()

        else:
            self.page_number += 1
            next_page_url = self.start_urls[0] + f'?page={self.page_number}'
            yield scrapy.Request(
                url=get_proxy_url(next_page_url),
                callback=self.parse, 
                meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_methods=self.page_methods,
                errback=self.errback,
            ))

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()
