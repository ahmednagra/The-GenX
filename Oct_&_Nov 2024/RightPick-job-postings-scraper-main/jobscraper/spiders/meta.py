import scrapy
from datetime import datetime

import json
from scrapy_playwright.page import PageMethod
from scrapy.crawler import CrawlerProcess
import logging
from markdownify import markdownify
from dataextraction import get_job_info, get_seen_jobs, get_id_unique, get_titles_and_urls
from collections import defaultdict
import os
from jobscraper.spiders import close_spider

class MetaSpider(scrapy.Spider):
    name = 'meta'
    start_urls = ['https://www.metacareers.com/graphql']

    log_dir = f'data/logs/{name}'
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = {
        'LOG_FILE': f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        'LOG_LEVEL': 'DEBUG',
        'LOG_ENABLED': True,
    }

    scraped_jobs_dict = dict()
    remove_obsolete_jobs = False


    # custom_settings = {
    #     'FEED_URI': '%(name)s.json',
    #     'FEED_FORMAT': 'json',
    #     'FEED_EXPORTERS': {
    #         'json': 'scrapy.exporters.JsonItemExporter',
    #     },
    #     'FEED_EXPORT_ENCODING': 'utf-8',
    # }

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.metacareers.com',
        'Referer': 'https://www.metacareers.com/jobs?teams[0]=Advertising%20Technology&teams[1]=AR%2FVR&teams[2]=Business%20Development%20and%20Partnerships&teams[3]=Communications%20and%20Public%20Policy&teams[4]=Data%20and%20Analytics&teams[5]=Data%20Centers&teams[6]=Design%20and%20User%20Experience&teams[7]=Enterprise%20Engineering&teams[8]=Global%20Operations&teams[9]=Infrastructure&teams[10]=Internship%20-%20Business&teams[11]=Internship%20-%20Engineering%2C%20Tech%20%26%20Design&teams[12]=Internship%20-%20PhD&teams[13]=Product%20Management&teams[14]=Research&teams[15]=Sales%20and%20Marketing&teams[16]=Security&teams[17]=Software%20Engineering&teams[18]=Technical%20Program%20Management&teams[19]=University%20Grad%20-%20Business&teams[20]=University%20Grad%20-%20Engineering%2C%20Tech%20%26%20Design&teams[21]=University%20Grad%20-%20PhD%20%26%20Postdoc',
        'X-Fb-Friendly-Name': 'CareersJobSearchResultsQuery',
        'X-Fb-Lsd': 'AVoDCkI51OQ',
    }

    payload = 'lsd=AVoDCkI51OQ&fb_api_caller_class=RelayModern&fb_api_req_friendly_name=CareersJobSearchResultsQuery&variables=%7B%22search_input%22%3A%7B%22q%22%3A%22%22%2C%22divisions%22%3A%5B%5D%2C%22offices%22%3A%5B%5D%2C%22roles%22%3A%5B%5D%2C%22leadership_levels%22%3A%5B%5D%2C%22saved_jobs%22%3A%5B%5D%2C%22saved_searches%22%3A%5B%5D%2C%22sub_teams%22%3A%5B%5D%2C%22teams%22%3A%5B%22Advertising%20Technology%22%2C%22AR%2FVR%22%2C%22Business%20Development%20and%20Partnerships%22%2C%22Communications%20and%20Public%20Policy%22%2C%22Data%20and%20Analytics%22%2C%22Data%20Centers%22%2C%22Design%20and%20User%20Experience%22%2C%22Enterprise%20Engineering%22%2C%22Global%20Operations%22%2C%22Infrastructure%22%2C%22Internship%20-%20Business%22%2C%22Internship%20-%20Engineering%2C%20Tech%20%26%20Design%22%2C%22Internship%20-%20PhD%22%2C%22Product%20Management%22%2C%22Research%22%2C%22Sales%20and%20Marketing%22%2C%22Security%22%2C%22Software%20Engineering%22%2C%22Technical%20Program%20Management%22%2C%22University%20Grad%20-%20Business%22%2C%22University%20Grad%20-%20Engineering%2C%20Tech%20%26%20Design%22%2C%22University%20Grad%20-%20PhD%20%26%20Postdoc%22%5D%2C%22is_leadership%22%3Afalse%2C%22is_remote_only%22%3Afalse%2C%22sort_by_new%22%3Afalse%2C%22page%22%3A1%2C%22results_per_page%22%3Anull%7D%7D&doc_id=6638667699485633'
    
    def start_requests(self):

        for url in self.start_urls:
            yield scrapy.Request(
                url,
                method='POST',
                headers=self.headers,
                body=self.payload,
                meta=dict(
                    sops_keep_headers=True,
                ),
                callback=self.parse
            )

    def parse(self, response):
        seen_jobs_set = get_seen_jobs(self.name)

        data = json.loads(response.body)

        if 'html' not in data:
            self.logger.warning(f'❌ Warning!! No html in response. Response: {data}')
        else:
            data = json.loads(data['html'])

        max_jobs = getattr(self,'max_jobs', len(data['data']['job_search']))
        max_jobs = int(max_jobs)
        self.remove_obsolete_jobs = max_jobs >= len(data['data']['job_search'])

        job_count = 0
        job_index = 0
        job_search_data = data['data']['job_search']

        while job_count < max_jobs and job_index < len(job_search_data):
            job = defaultdict(lambda: None, job_search_data[job_index])
            job_index += 1
            id_unique = get_id_unique(self.name, job)
            self.scraped_jobs_dict[id_unique] = job['title']
            if id_unique in seen_jobs_set:
                self.logger.info(f'👀 Job "{job["title"]}" already seen. Skipping...')
                continue
            
            job_count += 1
            job_url = f'https://www.metacareers.com/jobs/{job["id"]}/'
            
            initial_info = {
                'id_unique': id_unique,
                'url': job_url,
                'title': job['title'],
                'location': job['locations'], # [loc.split(',')[0] for loc in job['locations']],
                'id': job['id'],
                'teams': job['teams'],
                'sub_teams': job['sub_teams'],
            }

            yield response.follow(
                job_url, 
                method = 'GET',
                callback=self.parse_job, 
                meta=dict(
                    sops_render_js = True,
                    initial_info=initial_info,
                )
            )
        
        # write_seen_jobs(self.name, self.scraped_jobs_dict)


    def parse_job(self, response):
        initial_info = response.meta['initial_info']

        description = response.xpath('/html/body/div/div/div[2]/div/div[3]/div[2]/div/div/div[1]').get()
        
        if description:
            description = markdownify(description)
        else:
            self.logger.warning(f"🔔🔔🔔 No description found for job: {initial_info['title']} (URL: {response.url})")
            description = "No description available"
        
        related_jobs = response.xpath('/html/body/div/div/div[2]/div/div[3]/div[2]/div/div/div[2]/ul/li//a[contains(@href, "/jobs/")]').getall()
        related_jobs = get_titles_and_urls(related_jobs)

        job_posting_text = f"""Job title:\n {initial_info["title"]}
Teams:\n {initial_info["teams"]}
Sub-teams:\n {initial_info["sub_teams"]}
Description:\n
{description}
"""
        
        job_info = get_job_info(job_posting_text)
        
        further_info = {
            **initial_info,
            'description': description,
            'related_jobs': related_jobs,
            **job_info
        }

        yield further_info

    def close(self, reason):
        close_spider(self, reason)

# if __name__ == "__main__":
#     process = CrawlerProcess()
#     process.crawl(MetaSpider)
#     process.start()
