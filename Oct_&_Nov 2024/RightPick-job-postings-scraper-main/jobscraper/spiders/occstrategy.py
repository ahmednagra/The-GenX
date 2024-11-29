import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from dataextraction import get_job_info, get_seen_jobs, get_id_unique
import os
from jobscraper.spiders import close_spider

from markdownify import markdownify
import re

MAX_JOBS = 1_000_000


class OCCStrategySpider(scrapy.Spider):
    name = "occstrategy"
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
        url = 'https://careers.occstrategy.com/vacancies/vacancy-search-results.aspx'

        yield scrapy.Request(url, self.parse)

    def parse(self, response):
        job_links = response.css('.vsr-job__title a::attr(href)').getall()
        job_titles = response.css('.vsr-job__title a::text').getall()

        for title, link in zip(job_titles, job_links):
            link = response.urljoin(link)
            if self.fetched_count >= self.max_jobs:
                self.logger.info(f"🛑 Reached max jobs: {self.max_jobs}. Stopping...")
                return
            job_dict = {
                'url': link,
                'title': title,
            }
            id_unique = get_id_unique(
                self.name, job_dict, title=job_dict["title"]
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
            yield scrapy.Request(link, callback=self.detail_page, meta={'title': title})
        next_page_found = True
        next_page_text = response.css('#ctl00_ContentContainer_ctl01_VacancyPager2 a::text').getall()
        next_page_link = response.css('#ctl00_ContentContainer_ctl01_VacancyPager2 a::attr(href)').getall()
        viewstate_generator = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        viewstate = response.css('#__VIEWSTATE::attr(value)').get()
        event_validation = response.css('#__EVENTVALIDATION::attr(value)').get()
        for text, href in zip(next_page_text, next_page_link):
            if text == 'Next':
                next_page_href = href
                match = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", next_page_href)
                if match:
                    event_target = match.group(1)
                    event_argument = match.group(2)
                    next_page_found = True
                    self.logger.debug(f"✨ Next page found. Event Target: {event_target} | Event Argument: {event_argument} | Next Page Href: {next_page_href}")

                    # data = {
                    #     '__EVENTTARGET': event_target,
                    #     '__EVENTARGUMENT': event_argument,
                    #     '__LASTFOCUS': '',
                    #     '__VIEWSTATE': viewstate,
                    #     'ctl00$MyCandidateNavigation$LoginBoxMobile$ctl01$txtUsername': '',
                    #     'ctl00$MyCandidateNavigation$LoginBoxMobile$ctl01$txtPassword': '',
                    #     'ctl00$MyCandidateNavigation$LoginBoxMobile$ctl01$captcha$CaptchaFieldctl00_MyCandidateNavigation_LoginBoxMobile_ctl01_login': '',
                    #     'ctl00$MyCandidateNavigation$LoginBoxMobile$ctl01$hdnScreenWidth': '1366',
                    #     'ctl00$MyCandidateNavigation$LoginBox$ctl01$txtUsername': '',
                    #     'ctl00$MyCandidateNavigation$LoginBox$ctl01$txtPassword': '',
                    #     'ctl00$MyCandidateNavigation$LoginBox$ctl01$captcha$CaptchaFieldctl00_MyCandidateNavigation_LoginBox_ctl01_login': '',
                    #     'ctl00$MyCandidateNavigation$LoginBox$ctl01$hdnScreenWidth': '1366',
                    #     'ctl00$topContent$QuickSearch$Keywords': '',
                    #     'ctl00$topContent$QuickSearch$Postcode': '',
                    #     'ctl00$topContent$QuickSearch$hfLongitude': '',
                    #     'ctl00$topContent$QuickSearch$hfLatitude': '',
                    #     'ctl00$ContentContainer$ctl01$ddSortColumn': 'VacV.DatePosted',
                    #     'ctl00$FlagSelector$ctl00$rptFlags': 'en-GB',
                    #     'ctl00$FlagSelector$ctl00$ctl02$zoneList': 'GMT Standard Time',
                    #     'ctl00$ctl08$hiddenPostAction': 'False',
                    #     '__VIEWSTATEGENERATOR': viewstate_generator,
                    #     '__SCROLLPOSITIONX': '0',
                    #     '__SCROLLPOSITIONY': '0',
                    #     '__VIEWSTATEENCRYPTED': '',
                    #     '__EVENTVALIDATION': event_validation,
                    # }
                    data = {
                        '__EVENTTARGET': event_target,
                        '__EVENTARGUMENT': event_argument,
                        '__VIEWSTATE': viewstate,
                        '__VIEWSTATEGENERATOR': viewstate_generator,
                        '__EVENTVALIDATION': event_validation,
                    }
                    yield scrapy.FormRequest(
                        method='POST',
                        url='https://careers.occstrategy.com/vacancies/vacancy-search-results.aspx',
                        formdata=data,
                        callback=self.parse,
                        meta={
                            'sops_residential': True,
                            'sops_keep_headers': True,
                        },
                        dont_filter=True,
                    )
                else:
                    self.logger.warning(f"💫 No match found for next page link: {next_page_href}")
        if not next_page_found:
            self.logger.info("🛑 No next page found. Stopping...")
            return

    def detail_page(self, response):
        title = response.meta.get('title')
        location = []
        vacancy_deadline = response.css('[data-id="div_content_VacV_AdvertisingEndDate"] span::text').get()
        loc = response.css(
            '#ctl00_ctl00_ContentContainer_fullWidthContainer_fcVacancyDetails_VacV_LocationID::text').get()
        if loc:
            if "Boston/ New York (no preference)" in loc:
                location.extend(["Boston", "New York City"])
            else:
                location.append(loc)
        description = response.css('.fullwidth p').getall()
        description = '\n'.join(description)
        description = markdownify(description)

        job_posting_text = f"""Job title:\n {title}
        Description:\n{description}
        """
        job_info = get_job_info(job_posting_text)
        further_info = {
            'title': title,
            'url': response.url,
            'location': location,
            'vacancy_deadline': vacancy_deadline,
            'description': description
        }
        yield {**further_info, **job_info}

    def closed(self, reason):
        close_spider(self, reason)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(OCCStrategySpider)
    process.start()
