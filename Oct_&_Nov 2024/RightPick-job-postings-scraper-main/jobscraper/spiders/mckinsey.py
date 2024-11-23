import scrapy
from datetime import datetime

from collections import defaultdict
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
from markdownify import markdownify
import os
from jobscraper.spiders import close_spider

class MckinseySpider(scrapy.Spider):
    name = 'mckinsey'
    page_size = 1000
    start = 1
    url = 'https://mckapi.mckinsey.com/api/jobsearch?pageSize={page_size}&start={start}&interest=Analytics,Consulting,Design,Digital,Implementation,Transformation+%26+Turnaround'

    log_dir = f'data/logs/{name}'
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = {
        'LOG_FILE': f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        'LOG_LEVEL': 'DEBUG',
        'LOG_ENABLED': True,
    }

    scraped_jobs_dict = dict()
    remove_obsolete_jobs = False

    def start_requests(self):
        max_jobs = getattr(self,'max_jobs', None)
        if max_jobs is not None:
            self.max_jobs = int(max_jobs)
            page_size_param = max_jobs
        else:
            self.max_jobs = None
            page_size_param = self.page_size

        yield scrapy.Request(
            url=self.url.format(page_size=page_size_param, start=self.start),
            callback=self.parse)

    def parse(self, response):
        data = response.json()

        num_found = int(data['numFound'])
        if num_found > self.page_size and self.max_jobs is None:
            self.page_size *= 2
            yield scrapy.Request(
                url=self.url.format(page_size=self.page_size, start=self.start),
                callback=self.parse)
        else:
            seen_jobs_set = get_seen_jobs(self.name)
            job_count = 0
            job_index = 0
            job_search_data = data['docs']
            self.remove_obsolete_jobs = self.max_jobs is None

            while job_index < len(job_search_data) and (self.max_jobs is None or job_count < self.max_jobs):
                job = job_search_data[job_index]
                job_index += 1
                job = defaultdict(lambda: None, job)

                id_unique = get_id_unique(self.name, job)

                self.scraped_jobs_dict[id_unique] = job['friendlyURL']
                if id_unique in seen_jobs_set:
                    self.logger.info(f'👀 Job "{job["title"]}" already seen. Skipping...')
                    continue

                job_count += 1

                job_loader = dict()
                job_loader['id_unique'] = id_unique
                job_loader['job_id'] = job['jobID']
                job_loader['url_id'] = job['friendlyURL']
                job_loader['url'] = f"https://www.mckinsey.com/careers/search-jobs/jobs/{job['friendlyURL']}"
                job_loader['title'] = job['title']
                job_loader['job_skill_group'] = [job['jobSkillGroup']]
                job_loader['job_skill_code'] = [job['jobSkillCode']]
                job_loader['interest'] = job['interest']
                job_loader['interest_category'] = job['interestCategory']
                job_loader['location'] = [job['cities']]
                job_loader['functions'] = [job['functions']]
                job_loader['industries'] = [job['industries']]
                who_you_will_work_with = markdownify(job['whoYouWillWorkWith'])
                job_loader['who_you_will_work_with'] = who_you_will_work_with
                what_you_will_do = markdownify(job['whatYouWillDo'])
                job_loader['what_you_will_do'] = what_you_will_do
                your_background = markdownify(job['yourBackground'])
                job_loader['your_background'] = your_background
                job_loader['post_to_linkedin'] = job['postToLinkedIn']



                description = f"""Description:\n {what_you_will_do}
Who you will work with:\n {who_you_will_work_with}
Your background: {your_background}"""
                job_loader['description'] = description

                job_posting_text = f"""Job title:\n {job["title"]}
Job interest:\n {job["interest"]}
Job interest category:\n {job["interestCategory"]}
Job skill group:\n {job["jobSkillGroup"]}
Job skill code:\n {job["jobSkillCode"]}
Job functions:\n {job["functions"]}
{description}
"""
                job_posting_text = markdownify(job_posting_text)

                job_info = get_job_info(job_posting_text)
                job_loader = {**job_loader, **job_info}

                yield job_loader

            # write_seen_jobs(self.name, self.scraped_jobs_dict)

    def closed(self, reason):
        close_spider(self, reason)