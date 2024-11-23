import scrapy
import json
from collections import defaultdict
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from datetime import datetime
import os
from jobscraper.spiders import close_spider

class BCGSpider(scrapy.Spider):
    name = 'bcg'
    allowed_domains = ['careers.bcg.com']
    url = 'https://careers.bcg.com/widgets'

    log_dir = f'data/logs/{name}'
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = {
        'LOG_FILE': f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        'LOG_LEVEL': 'DEBUG',
        'LOG_ENABLED': True,
    }

    headers = {
        'Content-Type': 'application/json',
    }

    size = 300

    payload = '''{{"lang":"en_us","deviceType":"desktop","country":"us","pageName":"search-results","ddoKey":"refineSearch","sortBy":"","subsearch":"","from":0,"jobs":true,"counts":true,"all_fields":["country","city","category","company"],"size":{size},"clearAll":false,"jdsource":"facets","isSliderEnable":false,"pageId":"page17","siteType":"external","keywords":"","global":true,"selected_fields":{{"category":["Data Science and Analytics","Design Strategy","Consulting"]}},"locationData":{{}}}}'''

    scraped_jobs_dict = dict()
    remove_obsolete_jobs = False

    def start_requests(self):
        yield scrapy.Request(
            self.url,
            method='POST',
            headers=self.headers,
            meta={'sops_keep_headers': True},
            body=self.payload.format(size=self.size),
            callback=self.parse
        )

    def parse(self, response):
        data = response.json()

        if 'html' not in data:
            self.logger.warning(f'Warning!! No html in response. Data extracted from the "refineSearch" key.')
            # Extract job data directly from the 'refineSearch' key
            data = data.get('refineSearch', {})
        else:
            data = json.loads(data['html'])

        num_found = int(data.get('totalHits', 0))
        if num_found > self.size and getattr(self,'max_jobs', None) is None:
            self.size *= 2
            yield scrapy.Request(
                self.url,
                method='POST',
                headers=self.headers,
                meta={'sops_keep_headers': True},
                body=self.payload.format(size=self.size),
                callback=self.parse
            )
        else:
            seen_jobs_set = get_seen_jobs(self.name)
            job_search_data = data.get('data', {}).get('jobs', [])
            number_of_jobs = len(job_search_data)
            self.max_jobs = getattr(self,'max_jobs', number_of_jobs)
            self.max_jobs = int(self.max_jobs)
            self.remove_obsolete_jobs = self.max_jobs >= number_of_jobs


            job_count = 0
            job_index = 0

            while job_count < self.max_jobs and job_index < number_of_jobs:
                job = job_search_data[job_index]
                job_index += 1
                job = defaultdict(lambda: None, job)
                job['apply_url'] = job['applyUrl'] # Be careful, important to have this line before calling get_id_unique! Otherwise, apply_url will be None (and will also need this key in pipelines.py)

                id_unique = get_id_unique(self.name, job)
                self.scraped_jobs_dict[id_unique] = job['title']
                if id_unique in seen_jobs_set:
                    self.logger.info(f'👀 Job "{job["title"]}" already seen. Skipping...')
                    continue
                job_count += 1
                locations = [location['city'] for location in job['multi_location_array']]
                # # print locations with emojis
                # self.logger.info(f'📍 Locations: {locations}')

                if job['isMultiCategory']:
                    categories = [category['category'] for category in job['multi_category_array']]
                else:
                    categories = [job['category']]

                job_id = job['jobId']
                job_title = job['title']
                url = f'https://careers.bcg.com/job/{job_id}'

                initial_info = {
                    'url': url,
                    'title': job_title,
                    'location': locations,
                    'description_teaser': job['descriptionTeaser'],
                    'job_id': job_id,
                    'categories': categories,
                    'subcategory': job['subCategory'],
                    'posted_date': job['postedDate'],
                    'created_date': job['dateCreated'],
                    'skills': job['ml_skills'],
                    'industry': job['industry'],
                    'apply_url': job['applyUrl'],
                    'keyword_clicks': job['keywordClicks'], 
                    'location_clicks': job['locationClicks'],
                    'category_clicks': job['categoryClicks'], 
                    'landing_page_clicks': job['landingPageClicks'],
                    'total_search_clicks': job['totalSearchClicks']
                }

                yield response.follow(
                    url, 
                    method = 'GET',
                    callback=self.parse_job, 
                    meta = dict(
                        # sops_render_js = True,
                        # wait_for='.job-description',
                        initial_info = initial_info,
                        )
                    )

            # write_seen_jobs(self.name, self.scraped_jobs_dict)


    def parse_job(self, response):
        initial_info = response.meta['initial_info']
        # self.logger.info(f'👀 Scraping job: {initial_info["title"]}')
        # self.logger.info(f'Whole response: {response.text}')
        # self.logger.info(f'Response body: {response.css("body").get()}')
        # description = response.css('section.job-description').get()

        description = ""
        for script in response.xpath("//script[@type='application/ld+json']/text()").getall():
            data = json.loads(script)
            if "description" in data:
                description = data["description"]
                break

        if description:
            description = markdownify(markdownify(description)) # Double markdownify because the description is escaped twice
        else:
            self.logger.error(f"❌ Error!! Description is None for job {initial_info['url']}. Replacing it with teaser. Response:\n {response.text} \n\n")
            self.logger.info("🥸 Response XPATH: {}".format(response.xpath("//script[@type='application/ld+json']/text()").get()))
            description = initial_info['description_teaser']

        job_posting_text = f"""Job title:\n {initial_info["title"]}
Description:\n
{description}
"""
        
        job_info = get_job_info(job_posting_text)
        
        further_info = {
            **initial_info,
            'description': description,
            **job_info
        }

        yield further_info

    def closed(self, reason):
        close_spider(self, reason)