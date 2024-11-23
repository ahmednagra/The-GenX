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


class UberSpider(scrapy.Spider):
    name = "uber"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        # 'cookie': '_ua={"session_id":"bdf09609-6dbd-4ad5-981d-0f21207e67c4","session_time_ms":1725126138121}; marketing_vistor_id=3d272c2f-b851-420b-98d2-903f3d5d574a; user_city_ids=1005; uber_sites_geolocalization={%22best%22:{%22localeCode%22:%22en%22%2C%22countryCode%22:%22US%22%2C%22territoryId%22:1005}%2C%22url%22:{%22localeCode%22:%22en%22%2C%22countryCode%22:%22US%22}%2C%22user%22:{%22countryCode%22:%22PK%22%2C%22territoryId%22:1005%2C%22productGeofenceUUID%22:%224faff711-2233-407f-a2c8-19a34ef6785f%22%2C%22territoryGeoJson%22:[[{%22lat%22:31.7405758%2C%22lng%22:74.0033188}%2C{%22lat%22:31.7405758%2C%22lng%22:74.6546712}%2C{%22lat%22:31.2502766%2C%22lng%22:74.6546712}%2C{%22lat%22:31.2502766%2C%22lng%22:74.0033188}]]%2C%22territoryGeoPoint%22:{%22latitude%22:31.495426%2C%22longitude%22:74.38436}%2C%22localeCode%22:%22en%22}}; jwt-session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MjUxMjYxMzgsImV4cCI6MTcyNTIxMjUzOH0.lRizyAg0_iup0OQ52YFXMYhfVUrZyKk8InHN5k17WO0; utag_main__sn=1; utag_main_ses_id=1725126146470%3Bexp-session; utag_main__ss=0%3Bexp-session; _fbp=fb.1.1725126166148.426993746847288415; _gid=GA1.2.492302695.1725126173; _hjSession_960703=eyJpZCI6Ijk3MGJiOTYxLTRlMzAtNGY1Yy1iMjkxLWYxNWYyMWUwNzMxNyIsImMiOjE3MjUxMjYxNzQzNjgsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=; __cf_bm=Td0lysQBovw6PhP073Xro2l6OUqrZiniGNys8Tl31HQ-1725127590-1.0.1.1-QEq.XkjLawEflOb4BkGgADK7T38lMCkfMG8ajt33oj._3wtXnhjtfHb6se76IjVvjZzQ5y9oBSQ6ZXeZ4eqKwg; UBER_CONSENTMGR=1725127595087|consent:true; CONSENTMGR=1725127595087|consent:true; utag_main__pn=2%3Bexp-session; utag_main__se=4%3Bexp-session; utag_main__st=1725129395210%3Bexp-session; _hjSessionUser_960703=eyJpZCI6IjI0NzU1NjkwLTE3OTctNTdkNS1hNDRmLTA5YjVjMmU4OTRmMCIsImNyZWF0ZWQiOjE3MjUxMjYxNzQzNjcsImV4aXN0aW5nIjp0cnVlfQ==; _ga_W3V99WJNZ7=GS1.1.1725126164.1.1.1725127597.0.0.0; _gat_gtag_UA_7157694_35=1; _ga_XTGQLY6KPT=GS1.1.1725126171.1.1.1725127599.0.0.0; _ga=GA1.1.1801630787.1725126165',
        'origin': 'https://www.uber.com',
        'priority': 'u=1, i',
        'referer': 'https://www.uber.com/us/en/careers/list/?department=Business%20Development&department=Community%20Operations&department=Data%20Science&department=Design&department=Engineering&department=Marketing&department=Operations&department=Product&department=Sales%20%26%20Account%20Management&department=University',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'x-csrf-token': 'x',
    }
    def pay_load(self, page_no):
        json_data = {
            'params': {
                'department': [
                    'Business Development',
                    'Community Operations',
                    'Data Science',
                    'Design',
                    'Engineering',
                    'Marketing',
                    'Operations',
                    'Product',
                    'Sales & Account Management',
                    'University',
                ],
            },
            'limit': 10,
            'page': page_no,
        }
        return json.dumps(json_data)

    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    run_completed = False

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        page_no = 1
        jobs_viewed = 0
        json_data = self.pay_load(page_no)
        yield scrapy.Request(
            url="https://www.uber.com/api/loadSearchJobsResults?localeCode=en",
            method="POST",
            headers=self.headers,
            body=json_data,
            meta={
                'page_no': page_no, 
                'jobs_viewed': jobs_viewed,
                'sops_keep_headers': True,
            },
        )

    def parse(self, response):
        jobs_viewed = response.meta.get('jobs_viewed')
        page_no = response.meta.get('page_no')
        data = json.loads(response.body)
        jobs = data.get("data", {}).get("results", [])
        len_jobs = len(jobs) if jobs else 0
        self.remove_obsolete_jobs = self.max_jobs >= len_jobs
        total_jobs = data.get("data", {}).get("totalResults", {}).get('low')
        for job in jobs:
            jobs_viewed = jobs_viewed + 1
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            job_dict = {
                "title": job.get("title", "").strip(),
                "location": [
                    location.get("city") for location in job.get("allLocations", [])
                ],
                "id": job.get("id"),
                "url": f"https://www.uber.com/global/en/careers/list/{job.get('id')}/?uclick_id=9aa1e1fb-8683-41d5-8205-501d45c0bea5",
                "apply_link": f"https://www.uber.com/careers/apply/interstitial/{job.get('id')}?uclick_id=9aa1e1fb-8683-41d5-8205-501d45c0bea5",
                "description": markdownify(job.get("description", "")),
                "department": job.get("department"),
                "program_and_platform": job.get("programAndPlatform"),
                "level": job.get("level"),
                "creation_date": job.get("creationDate"),
                "team": job.get("team"),
                "portal_id": job.get("portalID"),
                "status_id": job.get("statusID"),
                "status_name": job.get("statusName"),
                "job_type": job.get("timeType"),
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
            job_posting_text = f"""Job title:\n {job_dict['title']}
                            Description:\n{job_dict['description']}
                            """
            job_info = get_job_info(job_posting_text)
            yield {**job_dict, **job_info}

        if jobs_viewed < total_jobs:
            page_no = page_no + 1
            json_data = self.pay_load(page_no)
            yield scrapy.Request(
                url="https://www.uber.com/api/loadSearchJobsResults?localeCode=en",
                method="POST",
                headers=self.headers,
                body=json_data,
                meta={
                    'page_no': page_no, 
                    'jobs_viewed': jobs_viewed,
                    'sops_keep_headers': True,
                },
            )

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(UberSpider)
    process.start()
