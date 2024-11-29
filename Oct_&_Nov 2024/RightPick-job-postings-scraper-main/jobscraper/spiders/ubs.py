import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
import os

from urllib.parse import urlencode
from jobscraper.spiders import close_spider
from markdownify import markdownify
import re
import json

MAX_JOBS = 1_000_000

class UbsSpider(scrapy.Spider):
    name = "ubs"
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    remove_obsolete_jobs = False
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    fetched_count = 0
    seen_jobs_count = 0
    scraped_jobs_dict = dict()
    run_completed = False



    def start_requests(self):
        self.seen_jobs_set = get_seen_jobs(self.name)
        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS) or MAX_JOBS)
        self.remove_obsolete_jobs = self.max_jobs >= MAX_JOBS
        url = 'https://jobs.ubs.com/TGnewUI/Search/home/HomeWithPreLoad?partnerid=25008&siteid=5012&PageType=searchResults&SearchType=linkquery&LinkID=6015#keyWordSearch=&locationSearch='
        yield scrapy.Request(url, callback=self.parse,)

    def parse(self, response):
        cookies = response.headers.getlist('Set-Cookie')
        cookies_str = b''.join(cookies).decode('utf-8')
        # Extract the cookie values using regular expressions and group(1) to extract the actual value
        tg_session_25008_5012 = re.search(r'tg_session_25008_5012=([^;]+)', cookies_str)
        tg_session = re.search(r'tg_session=([^;]+)', cookies_str)
        tg_rft = re.search(r'tg_rft=([^;]+)', cookies_str)
        tg_rft_mvc = re.search(r'tg_rft_mvc=([^;]+)', cookies_str)

        sessionid = response.css('#sessionid::attr(value)').get()
        partner_id = response.css('#partnerId::attr(value)').get()
        siteId = response.css('#siteId::attr(value)').get()
        link_id = response.css('#linkId::attr(value)').get()
        rft = response.css('[name="__RequestVerificationToken"]::attr(value)').get()

        url = 'https://jobs.ubs.com/TgNewUI/Search/Ajax/MatchedJobs'
        cookies = {
            'tg_session_25008_5012': tg_session_25008_5012.group(1) if tg_session_25008_5012 else '',
            'tg_session': tg_session.group(1) if tg_session else '',
            'tg_rft': tg_rft.group(1) if tg_rft else '',
            'tg_rft_mvc': tg_rft_mvc.group(1) if tg_rft_mvc else '',
            'geo-country': 'PK',
            'sat_track': 'true',
            's_lv_s': 'First%20Visit',
            'AMCVS_73FAC51D54C72AE50A4C98BC%40AdobeOrg': '1',
            's_ecid': 'MCMID%7C75522721953618165144521412867917598680',
            's_visit': '1',
            's_mtouch': 'Internal',
            's_cc': 'true',
            'aam_uuid': '75493351460292711974518490112521156743',
            'AAMSID': 'AAM%3D21740122%3A21739909',
            'AMCV_73FAC51D54C72AE50A4C98BC%40AdobeOrg': '770003774%7CMCIDTS%7C19984%7CMCMID%7C75522721953618165144521412867917598680%7CMCAAMLH-1727203260%7C3%7CMCAAMB-1727203260%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1726605660s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.5.0%7CMCCIDH%7C-1236019896',
            's_ppn': 'https%3A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers%2Fmeet-us.html',
            's_gpv_url2': 'https%3A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers.html',
            's_gpv_channel': 'global%3Acareers%3Ameet%20us%3Aresponsive_content',
            's_gpv_url': 'https%3A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers%2Fmeet-us.html',
            's_nr': '1726598461275-New',
            's_lv': '1726598461275',
            's_ppvl': 'https%253A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers%2Fmeet-us.html%2C53%2C5%2C1741%2C1366%2C641%2C1366%2C768%2C1%2CP',
            'scrollTracking': 'true',
            's_ht': '1726598485291',
            's_hc': '1%7C0%7C1%7C0%7C2',
            's_ppv': 'https%253A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers%2Fmeet-us.html%2C100%2C53%2C3295%2C1366%2C641%2C1366%2C768%2C1%2CP',
            's_sq': 'ubs-loglive%3D%2526c.%2526a.%2526activitymap.%2526page%253Dhttps%25253A%25252F%25252Fwww.ubs.com%25252Fglobal%25252Fen%25252Fcareers%25252Fmeet-us.html%2526link%253DOK%2526region%253Ddoc%2526.activitymap%2526.a%2526.c%2526pid%253Dhttps%25253A%25252F%25252Fwww.ubs.com%25252Fglobal%25252Fen%25252Fcareers%25252Fmeet-us.html%2526oid%253DOK%2526oidt%253D3%2526ot%253DSUBMIT',
            'ubs_cookie_settings_2.0.4': '0-4-3-2-1',

        }

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            # 'Cookie': 'tg_session_25008_5012=^TAmfhlbvqGGm3Vo9KBWxZJqolgCqcCoXPEO9qDwlTkJyhEFcx7zrQeseu8KOyFsEjx3D3GAbMVOcU7Agx7LOq90ub7wGGK354CKkKngCOH4=; tg_session=^TAmfhlbvqGGm3Vo9KBWxZJqolgCqcCoXPEO9qDwlTkJyhEFcx7zrQeseu8KOyFsEjx3D3GAbMVOcU7Agx7LOq90ub7wGGK354CKkKngCOH4=; tg_rft=^mHOwVU4ei7NrQmF6n2llJb/gAewiggqW8DkdZR4sm9lKjwoWo3Nf4D4dN8aqigFzzljy446oEvDpcUWUpgMSEPcZ3tSH90ojDHue1sa6zgo=; tg_rft_mvc=1RJwHo4bG8T09Qfvi1Y9tZfyUGF-1x_sH9RnYG840X_fXpW418Lv--2T9duP2H-mrx7OFxrxOIzX3CJ5Zd8NJgIQ0fV_7vn-LEaw4mFgapF1QEHM4C7gByEUjlYhIR55MY_IJA2; geo-country=PK; sat_track=true; s_lv_s=First%20Visit; AMCVS_73FAC51D54C72AE50A4C98BC%40AdobeOrg=1; s_ecid=MCMID%7C75522721953618165144521412867917598680; s_visit=1; s_mtouch=Internal; s_cc=true; aam_uuid=75493351460292711974518490112521156743; AAMSID=AAM%3D21740122%3A21739909; AMCV_73FAC51D54C72AE50A4C98BC%40AdobeOrg=770003774%7CMCIDTS%7C19984%7CMCMID%7C75522721953618165144521412867917598680%7CMCAAMLH-1727203260%7C3%7CMCAAMB-1727203260%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1726605660s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.5.0%7CMCCIDH%7C-1236019896; s_ppn=https%3A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers%2Fmeet-us.html; s_gpv_url2=https%3A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers.html; s_gpv_channel=global%3Acareers%3Ameet%20us%3Aresponsive_content; s_gpv_url=https%3A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers%2Fmeet-us.html; s_nr=1726598461275-New; s_lv=1726598461275; s_ppvl=https%253A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers%2Fmeet-us.html%2C53%2C5%2C1741%2C1366%2C641%2C1366%2C768%2C1%2CP; scrollTracking=true; s_ht=1726598485291; s_hc=1%7C0%7C1%7C0%7C2; s_ppv=https%253A%2F%2Fwww.ubs.com%2Fglobal%2Fen%2Fcareers%2Fmeet-us.html%2C100%2C53%2C3295%2C1366%2C641%2C1366%2C768%2C1%2CP; s_sq=ubs-loglive%3D%2526c.%2526a.%2526activitymap.%2526page%253Dhttps%25253A%25252F%25252Fwww.ubs.com%25252Fglobal%25252Fen%25252Fcareers%25252Fmeet-us.html%2526link%253DOK%2526region%253Ddoc%2526.activitymap%2526.a%2526.c%2526pid%253Dhttps%25253A%25252F%25252Fwww.ubs.com%25252Fglobal%25252Fen%25252Fcareers%25252Fmeet-us.html%2526oid%253DOK%2526oidt%253D3%2526ot%253DSUBMIT; ubs_cookie_settings_2.0.4=0-4-3-2-1; tg_rft=^mHOwVU4ei7NrQmF6n2llJb/gAewiggqW8DkdZR4sm9lKjwoWo3Nf4D4dN8aqigFzzljy446oEvDpcUWUpgMSEPcZ3tSH90ojDHue1sa6zgo=',
            'Origin': 'https://jobs.ubs.com',
            'RFT': rft,
            'Referer': 'https://jobs.ubs.com/TGnewUI/Search/home/HomeWithPreLoad?partnerid=25008&siteid=5012&PageType=searchResults&SearchType=linkquery&LinkID=6015',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        payload = {
            'PartnerId': partner_id,
            'SiteId': siteId,
            'Keyword': '',
            'Location': '',
            'KeywordCustomSolrFields': 'FORMTEXT2,FORMTEXT21,AutoReq,Department,JobTitle',
            'LocationCustomSolrFields': 'FORMTEXT2,FORMTEXT23,Location',
            'TurnOffHttps': False,
            'LinkID': link_id,
            'Latitude': 0,
            'Longitude': 0,
            'EncryptedSessionValue': sessionid,
            'FacetFilterFields': {
                'Facet': [
                    {
                        'Name': 'formtext21',
                        'Description': 'Function Category',
                        'Options': [
                            {
                                'OptionName': 'Digital',
                                'OptionValue': 'Digital',
                                'Count': 1,
                                'Selected': True,
                            },
                            {
                                'OptionName': 'Equities',
                                'OptionValue': 'Equities',
                                'Count': 1,
                                'Selected': True,
                            },
                            {
                                'OptionName': 'Investment banking',
                                'OptionValue': 'Investment Banking',
                                'Count': 9,
                                'Selected': True,
                            },
                            {
                                'OptionName': 'Portfolio and fund management',
                                'OptionValue': 'Product and Portfolio Management',
                                'Count': 2,
                                'Selected': True,
                            },
                            {
                                'OptionName': 'Quantitative analysis',
                                'OptionValue': 'Quantitative Analysis',
                                'Count': 2,
                                'Selected': True,
                            },
                            {
                                'OptionName': 'Research',
                                'OptionValue': 'Research & Analysis',
                                'Count': 3,
                                'Selected': True,
                            },
                            {
                                'OptionName': 'Sales',
                                'OptionValue': 'Sales',
                                'Count': 2,
                                'Selected': True,
                            },
                            {
                                'OptionName': 'Strategy',
                                'OptionValue': 'Strategy',
                                'Count': 1,
                                'Selected': True,
                            },
                            {
                                'OptionName': 'Trading',
                                'OptionValue': 'Trading',
                                'Count': 1,
                                'Selected': True,
                            },
                        ],
                        'AriaLabel_FilterResultsByFacet': 'Filter search results by Function Category',
                        'SelectedCount': 9,
                    },
                ],
            },
            'PowerSearchOptions': {
                'PowerSearchOption': [
                    {
                        'VerityZone': 'FORMTEXT2',
                        'Type': 'multi-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT21',
                        'Type': 'multi-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'Department',
                        'Type': 'select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT22',
                        'Type': 'multi-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT23',
                        'Type': 'multi-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT10',
                        'Type': 'single-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT53',
                        'Type': 'single-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT56',
                        'Type': 'single-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT31',
                        'Type': 'single-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT19',
                        'Type': 'multi-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT28',
                        'Type': 'single-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'FORMTEXT4',
                        'Type': 'single-select',
                        'OptionCodes': [],
                    },
                    {
                        'VerityZone': 'LastUpdated',
                        'Type': 'date',
                        'Value': None,
                    },
                    {
                        'VerityZone': 'languagelist',
                        'Type': 'multi-select',
                        'OptionCodes': [],
                    },
                ],
            },
            'SortType': 'LastUpdated',
        }

        yield scrapy.Request(url, method='POST', callback=self.parse_detail, body=json.dumps(payload), headers=headers, cookies=cookies, meta={'sops_keep_headers': True, 'sops_residential': True, })

    def parse_detail(self, response):
        data = json.loads(response.text)
        jobs = data.get('Jobs', {}).get('Job', [])
        id = ''
        lastupdated = ''
        department = ''
        description = ''
        title = ''
        category = ''

        for job in jobs:
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            url = job.get('Link')
            questions = job.get('Questions')
            for question in questions:
                if question.get('QuestionName') == 'reqid':
                    id = question.get('Value')
                if question.get('QuestionName') == 'lastupdated':
                    lastupdated = question.get('Value')
                if question.get('QuestionName') == 'department':
                    department = question.get('Value')
                if question.get('QuestionName') == 'jobdescription':
                    description = question.get('Value')
                    description = markdownify(description)
                if question.get('QuestionName') == 'jobtitle':
                    title = question.get('Value')
                if question.get('QuestionName') == 'formtext21':
                    category = question.get('Value')
            job_dict = {
                'id': str(id),
                'url': url,
                'title': title,

            }
            id_unique = get_id_unique(
                self.name, job_dict, id=job_dict["id"], title=job_dict["title"]
            )
            self.scraped_jobs_dict[id_unique] = job_dict["title"]
            self.seen_jobs_count += 1
            if id_unique in self.seen_jobs_set:
                self.logger.info(
                    f'👀 Job "{job_dict["title"]}" already seen. Skipping...'
                )
                continue
            self.fetched_count += 1
            yield scrapy.Request(url, callback=self.detail_page,meta={'id': id, 'lastupdated': lastupdated,
                                                                      'department': department, 'description': description, 'title': title,
                                                                      'category': category})
    def detail_page(self, response):
        id = response.meta.get('id')
        lastupdated = response.meta.get('lastupdated')
        department = response.meta.get('department')
        description = response.meta.get('description')
        title = response.meta.get('title')
        category = response.meta.get('category')
        data = json.loads(response.css('#preLoadJSON::attr(value)').get())
        questions = data.get('Jobdetails').get('JobDetailQuestions')
        locations = []
        for question in questions:
            if question.get('QuestionName') == 'City':
                city = question.get('AnswerValue')
                locations.append(city)



        further_info = {
            'id': str(id),
            'title': title,
            'lastupdated': lastupdated,
            'department': department,
            'description': description,
            'url': response.url,
            'location': locations,
            'category': category

        }
        job_posting_text = f"""Job title:\n {further_info['title']}
                                                            Description:\n{further_info['description']}
                                                                            """
        job_info = get_job_info(job_posting_text)

        yield {**further_info, **job_info}
    def closed(self, reason):
        close_spider(self, reason)

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(UbsSpider)
    process.start()
