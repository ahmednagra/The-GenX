import scrapy
from datetime import datetime
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
from collections import defaultdict
import json
import os
from jobscraper.spiders import close_spider

MAX_JOBS = 1_000_000


class BainSpider(scrapy.Spider):
    name = "bain"
    # Updated to use the API URL instead of the old web scraping URL
    start_urls = [
        'https://www.bain.com/en/api/jobsearch/keyword/get?start=0&results=1&filters=workareas(1831443,1831445,1831447)'
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
        # Parse the JSON response from the API
        data = json.loads(response.text)

        # Get the list of seen jobs
        seen_jobs_set = get_seen_jobs(self.name)
        current_jobs_count = 0
        self.max_jobs = getattr(self, 'max_jobs', MAX_JOBS)
        self.max_jobs = int(self.max_jobs)
        self.remove_obsolete_jobs = self.max_jobs >= MAX_JOBS

        # Check if we need to request all jobs (from the new code)
        total_results = data.get('totalResults')
        current_results = response.url.split('results=')[1].split('&')[0]
        if total_results and current_results == '1':
            total_results_url = f'https://www.bain.com/en/api/jobsearch/keyword/get?start=0&results={total_results}&filters=workareas(1831443,1831445,1831447)'
            yield scrapy.Request(total_results_url, callback=self.parse)

        # Process the job listings from the API data
        if 'results' in data:
            job_count = 0
            job_index = 0
            jobs = data['results']

            while job_count < self.max_jobs and job_index < len(jobs):
                job = jobs[job_index]
                job_index += 1

                title = job.get('JobTitle')
                url = job.get('Link')
                # apply_url = ""  # As apply url was not available in json, we will fetch it from listing_page

                # apply url is not available in json, we can construct it manually
                job_id = job.get('JobId')
                apply_url = f"https://careers.bain.com/jobs/Login?folderId={job_id}"

                job_info = {"url": url, "title": title, "apply_url": apply_url}
                job_info = defaultdict(lambda: None, job_info)
                id_unique = get_id_unique(self.name, job_info)

                self.scraped_jobs_dict[id_unique] = job_info['title']

                if id_unique in seen_jobs_set:
                    self.logger.info(f'👀 Job "{job_info["title"]}" already seen. Skipping...')
                    continue

                job_count += 1
                current_jobs_count += 1

                initial_info = {"url": url, "title": title, "apply_url": apply_url}

                # Fetch more detailed job info if needed
                yield response.follow(
                    url,
                    callback=self.parse_job,
                    meta=dict(
                        sops_render_js=True,
                        initial_info=initial_info,
                    )
                )

        # if current_jobs_count < self.max_jobs:
        #     next_page = response.css("a.paginationNextLink::attr(href)").get()
        #     if next_page:
        #         yield response.follow(next_page, callback=self.parse)

        # write_seen_jobs(self.name, self.scraped_jobs_dict)

    def parse_job(self, response):
        initial_info = response.meta["initial_info"]

        # initial_info['title'] = response.xpath('//div[div[contains(text(),"Job Title")]]/div[@class="article__content__view__field__value"]/text()').get().strip()

        initial_info['id'] = int(response.css('.job-id+ p::text').get().strip())

        initial_info['areas_of_work'] = response.css('.category+ p').xpath('string()').get()
        initial_info['areas_of_work'] = initial_info['areas_of_work'].strip() if initial_info['areas_of_work'] else None

        initial_info['employment_type'] = response.css('.employment+ p::text').get()
        initial_info['employment_type'] = initial_info['employment_type'].strip() if initial_info['employment_type'] else None

        locations = response.css('.showLessLinkParagraph').xpath('string()').get()
        cleaned_locations = locations.replace("Show less", "").strip()
        initial_info['location'] = [loc.strip() for loc in cleaned_locations.split("|")]

        description = response.css('.title-with-rte-block__wrapper::text').get()
        description = markdownify(description)

        job_posting_text = f"""Job title:\n {initial_info["title"]}
                                Description:\n
                                {description}
                            """
        # initial_info['apply_url'] = response.css('.career-cta-btn a::attr(href)').get()
        job_info = get_job_info(job_posting_text)
        yield {**initial_info, "description": description, **job_info}

    def closed(self, reason):
        close_spider(self, reason)
