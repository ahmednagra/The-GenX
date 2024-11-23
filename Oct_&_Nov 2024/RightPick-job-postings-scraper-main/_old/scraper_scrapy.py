import scrapy
from scrapy_splash import SplashRequest 
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.exporters import JsonItemExporter
import logging
import random 
import json

logger = logging.getLogger(__name__)

class MckinseyJobsSpider(scrapy.Spider):
  name = "mckinsey_jobs"
  start_url = "https://www.mckinsey.com/careers/search-jobs"
  jobs = {}
  current_page = 1
  user_agents = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0"  
  ]
  
  def start_requests(self):
    url = f'{self.start_url}?page={self.current_page}'
    agent = random.choice(self.user_agents)
    headers = {
      'User-Agent': agent 
    }
    yield SplashRequest(
        url,
        self.start_parse,
        endpoint='render.html',
        args={
            'wait': 0.5,
            'http_headers': headers
        }
      )
    
  def start_parse(self, response):
    total_results_text = response.css('div.job-count-container > p ::text').get()
    if total_results_text: 
      total_results = int(total_results_text)
    else:
      logger.warning('Could not parse total results, defaulting to 500')
      total_results = 500
    total_pages = (total_results / 20) + 1
    self.parse(response) 
    if self.current_page < total_pages: 
      self.current_page += 1
      url = f'{self.start_url}?page={self.current_page}'
      agent = random.choice(self.user_agents)
      headers = {
        'User-Agent': agent 
      }
      yield SplashRequest(url, callback=self.parse, headers=headers) 
  
  def parse(self, response):
    for idx, job in enumerate(response.css('ul.job-list li.job-listing')):
      job_id = f'job_{idx}'
      print("🟥🟥🟥🟥🟥🟥 JOB: ", job_id)
      title = job.css('h2.headline a ::text').get()
      location = job.css('div.city-list-container li.city ::text').get()
      description = job.css('p.description ::text').get()
      print("🟥🟥🟥🟥🟥🟥 DESCRIPTION: ", description)
      interest = job.css('p.interests ::text').get()
      seniority = self.get_seniority(title) 
      url = job.css('h2.headline a::attr(href)').get()
      
      self.jobs[job_id] = {
        'title': title,
        'url': url,
        'seniority': seniority,
        'location': location or 'N/A', 
        'description': description or 'N/A',
        'interest': interest or 'N/A' 
      }
      yield self.jobs[job_id]
      
  def closed(self, reason):
    logger.info(f"Scraped {len(self.jobs)} jobs")
    
  def get_seniority(self, title):
    if 'intern' in title.lower():
      return 'intern'
    elif 'analyst' in title.lower():
      return 'analyst' 
    elif 'associate' in title.lower():
      return 'associate'
    elif 'manager' in title.lower():
      return 'manager'
    else:
      return 'n/a'

class MckinseyJobsPipeline:
  def __init__(self):
      self.file = open('mckinsey_jobs.json', 'w')
      self.file.write('[')
      self.logger.info("🟥🟥🟥🟥🟥🟥 PIPELINE INITIALIZED 🟥🟥🟥🟥🟥🟥")

  def process_item(self, item, spider): 
    line = json.dumps(spider.jobs) + ",\n"
    self.file.write(line)
    self.logger.info("🟥🟥🟥🟥🟥🟥 PIPELINE PROCESSING 🟥🟥🟥🟥🟥🟥")
    return item

  def close_spider(self, spider):
    self.file.write(']')
    self.file.close()

if __name__ == "__main__": 
  process = CrawlerProcess(get_project_settings())
  process.crawl(MckinseyJobsSpider)
  process.start() 
