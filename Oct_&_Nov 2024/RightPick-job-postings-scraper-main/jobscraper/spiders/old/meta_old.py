import scrapy
from datetime import datetime

from scrapy.crawler import CrawlerProcess
import os

class MetaAISpider(scrapy.Spider):
    name = "meta0"
    start_urls = ['https://www.metacareers.com/jobs']

    log_dir = f'data/logs/{name}'
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = {
        'FEED_URI': '%(name)s.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORTERS': {
            'csv': 'scrapy.exporters.CsvItemExporter',
        },
        'FEED_EXPORT_ENCODING': 'utf-8',
        'LOG_FILE': f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        'LOG_LEVEL': 'DEBUG',
        'LOG_ENABLED': True,
    }
    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                self.parse,
                meta=dict(playwright=True)
            )

    async def parse(self, response):
        graphql_request = {
            "query": """
                query {
                job_search {
                    id
                    title
                    locations
                    teams
                    sub_teams
                }
                }
            """
        }


        json_response = await response.playwright_page.evaluate("""async ({graphql_request}) => {
            const response = await fetch('https://www.metacareers.com/graphql', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(graphql_request),
            });
            return response.json();
        }""", {'graphql_request': graphql_request})

        # Now, we can parse the response
        for job in json_response['data']['job_search']:
            yield {
                'id': job['id'],
                'title': job['title'],
                'locations': job['locations'],
                'teams': job['teams'],
                'sub_teams': job['sub_teams'],
            }

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MetaAISpider)
    process.start()
