import html
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


class OliverwymanSpider(scrapy.Spider):
    name = "oliverwyman"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    run_completed = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        # 'cookie': 'VISITED_LANG=en; VISITED_COUNTRY=global; Per_UniqueID=191a524919475a-100200-58e2-191a5249195a1f; osano_consentmanager_uuid=c6087424-939e-4b6b-83b3-5d37a2b24045; osano_consentmanager=GZeHpQVfezB8Z5TctdwNawTGTZbw_pYhNuUTPS-Jcu9WGvXUPY_S7oSLIFj23BBtmsGzQ5T1hUtuQvSPgBUt8fyxFobKMm6JYjm5yKa4EDY2s1rqa-2ztiKAKUpCfKgXbp_Fib9zxo4CmYwpyXoHBA7NzmxMAsPmBtPn8S93kcbcPN3Gsx_xo08hazU9vaE5tQN8AOtFngevnie08siY9xMESvHdhkv8DVBRpIJzhYSuEK0bxfgY5zYaewD6JVi_Pe9XL0EiNAA2dg-qlL3VbC916C-B9O5s0eS2ww==; PLAY_SESSION=eyJhbGciOiJIUzI1NiJ9.eyJkYXRhIjp7IkpTRVNTSU9OSUQiOiJmZGFmNzc3Zi02YThjLTRlNTItYjZkMi0xOGYyZjM5Zjk0ZGMifSwibmJmIjoxNzI1MDk5ODE3LCJpYXQiOjE3MjUwOTk4MTd9.FjowMuIAR84edDYB5I4_4na0nzctALbMa8Jv9mOXAzg; PHPPPE_ACT=fdaf777f-6a8c-4e52-b6d2-18f2f39f94dc; PHPPPE_GCC=a; ext_trk=pjid%3Dfdaf777f-6a8c-4e52-b6d2-18f2f39f94dc&uid%3D191a524919475a-100200-58e2-191a5249195a1f&p_lang%3Den_global&refNum%3DMAMCGLOBAL; PHPPPE_ACT=fdaf777f-6a8c-4e52-b6d2-18f2f39f94dc',
        'origin': 'https://careers.marshmclennan.com',
        'priority': 'u=1, i',
        'referer': 'https://careers.marshmclennan.com/global/en/oliver-wyman-search',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'x-csrf-token': '236acd5c4f5c498daa36c2a1907b786d'
    }
    json_data = {
            "lang": "en_global",
            "deviceType": "desktop",
            "country": "global",
            "pageName": "Oliver Wyman Search",
            "ddoKey": "eagerLoadRefineSearch",
            "sortBy": "",
            "subsearch": "",
            "from": 0,
            "jobs": True,
            "counts": True,
            "all_fields": [
                "category",
                "country",
                "state",
                "city",
                "timeType",
                "business",
                "workFromHome",
                "campus",
                "jobType",
                "phLocSlider"
            ],
            "pageType": "landingPage",
            "size": 1000,
            "rk": "l-oliver-wyman-search",
            "clearAll": False,
            "jdsource": "facets",
            "isSliderEnable": True,
            "pageId": "page52-prod",
            "siteType": "external",
            "keywords": "",
            "global": True,
            "selected_fields": {
                "businessUnitDescr": [
                    "Oliver Wyman Group"
                ],
                "category": [
                    "Consulting"
                ]
            },
            "locationData": {
                "sliderRadius": 25,
                "aboveMaxRadius": False,
                "LocationUnit": "miles"
            },
            "rkstatus": True,
            "s": "1"
        }
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()

    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        yield scrapy.Request(
            "https://careers.marshmclennan.com/widgets",
            headers=self.headers,
            body=json.dumps(self.json_data),
            method="POST",
            meta={'sops_keep_headers': True},
        )

    def parse(self, response):
        jobs = (
            json.loads(response.body)
            .get("eagerLoadRefineSearch", {})
            .get("data", {})
            .get("jobs", [])
        )
        total_jobs = len(jobs)
        self.remove_obsolete_jobs = self.max_jobs >= total_jobs

        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            job_dict = {
                "id": job.get("jobId"),
                "title": job.get("title"),
                "url": f"https://careers.marshmclennan.com/global/en/job/{job.get('jobId')}",
                "apply_link": job.get("applyUrl"),
                "posted_date": job.get("postedDate"),
                "job_type": job.get("type"),
                "category": job.get("category"),
                "sub_category": job.get("subCategory"),
                "skills": job.get("ml_skills"),
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
                url=job_dict["url"],
                headers=self.headers,
                callback=self.job_detail,
                meta={"job_dict": job_dict},
            )

    def job_detail(self, response):
        data = json.loads(
            response.css('script[type="application/ld+json"]::text').get("")
        )
        further_info = response.meta["job_dict"]
        cities = []
        if isinstance(data.get("jobLocation"), dict):
            cities = [
                data.get("jobLocation", {})
                .get("address", {})
                .get("addressLocality", "")
            ]
        elif isinstance(data.get("jobLocation"), list):
            cities = [
                address.get("address", {}).get("addressLocality", "")
                for address in data.get("jobLocation", {})
            ]
        further_info["location"] = cities
        further_info["description"] = markdownify(
            html.unescape(data.get("description"))
        )
        job_posting_text = f"""Job title:\n {further_info['title']}
                                Skills:\n {further_info['skills']}
                                Description:\n{further_info['description']}
                                """
        job_info = get_job_info(job_posting_text)
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(OliverwymanSpider)
    process.start()
