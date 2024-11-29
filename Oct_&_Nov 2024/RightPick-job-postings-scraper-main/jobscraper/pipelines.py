# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem, CloseSpider, NotConfigured
import datetime
# import mysql.connector
from dataextraction import get_id_unique, get_locations
import os
from dotenv import load_dotenv, find_dotenv
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from jobsupdate.collection_update import upload_job, remove_obsolete_jobs
from jobscraper.spiders import COMPANY_TITLES

class JobPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if not adapter.get('company'):
            adapter['company'] = spider.name
        if not adapter.get('company_title'):
            adapter['company_title'] = COMPANY_TITLES[adapter['company']]

        if not adapter.get('date_scraped'):
            adapter['date_scraped'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if not adapter.get('id_unique'):
            adapter['id_unique'] = get_id_unique(spider.name, item)

        if adapter.get('location'):
            if isinstance(adapter['location'], list):
                job_title = adapter.get('title', '')
                job_description = adapter.get('description', '')
                adapter['location'] = get_locations(adapter['location'], (job_title, job_description))
                adapter['location_list_flattened'] = list(set(item for sublist in adapter['location'] for item in sublist))
            else:
                adapter['location_list_flattened'] = adapter['location']
        else:
            # If location is not present, try to infer it from title and description
            job_title = adapter.get('title', '')
            job_info = {key: value for key, value in adapter.items() if value}
            job_description = '\n\n'.join(f"{key}: {value}" for key, value in job_info.items())
            adapter['location'] = get_locations([], (job_title, job_description))
            adapter['location_list_flattened'] = list(set(item for sublist in adapter['location'] for item in sublist))
        
        return item


class DuplicatesPipeline:

    def __init__(self):
        self.names_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('id_unique'):
            id_unique = adapter['id_unique']
        else:
            id_unique = get_id_unique(spider.name, item)
        
        if id_unique in self.names_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.names_seen.add(id_unique)
            return item



class FirestoreUpdatePipeline:
    def __init__(self):
        # Check if the default app has already been initialized
        if not firebase_admin._apps:
            # Initialize Firebase Admin SDK with the service account from the environment variable.
            cred = credentials.Certificate(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def process_item(self, item, spider):
        # Convert item to dict and use the upload_job function to upload to Firestore
        upload_job(self.db, dict(item))
        return item
    

class RemoveOutdatedJobsPipeline:
    def __init__(self, stats):
        # Check if the default app has already been initialized
        if not firebase_admin._apps:
            # Initialize Firebase Admin SDK with the service account from the environment variable.
            cred = credentials.Certificate(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def close_spider(self, spider):
        if not getattr(spider, 'remove_obsolete_jobs', False):
            spider.logger.info('📊 remove_obsolete_jobs attribute is False: no obsolete job removed.')
            return
        
        # Get relevant stats
        close_reason = self.stats.get_value('finish_reason')
        item_scraped_count = self.stats.get_value('item_scraped_count', 0)
        error_count = sum(self.stats.get_value(f'log_count/{level}', 0) for level in ['ERROR', 'CRITICAL'])
        exception_count = sum(self.stats.get_value(f'spider_exceptions/{exc}', 0) for exc in self.stats.get_stats())

        # spider.logger.info(f'Spider close reason: {close_reason}')
        spider.logger.info(f'Items scraped: {item_scraped_count}')
        spider.logger.info(f'Errors logged: {error_count}')
        spider.logger.info(f'Exceptions caught: {exception_count}')

        # Check if the spider completed successfully without issues
        if error_count == 0 and exception_count == 0:
            spider.logger.info('🚮 Starting obsolete jobs removal...')
            scraped_jobs_dict = getattr(spider, 'scraped_jobs_dict', dict())
            company = spider.name
            remove_obsolete_jobs(company, spider, self.db, scraped_jobs_dict)
        else:
            spider.logger.warning('🚫 Spider did not complete its run successfully or encountered issues. Skipping obsolete job removal.')

# class SavingToMySQLPipeline(object):

#     def __init__(self):
#         self.create_connection()

#     def create_connection(self):
#         self.conn = mysql.connector.connect(
#             host = 'localhost',
#             user = 'root',
#             password = '123456',
#             database = 'db_name'
#         )
#         self.curr = self.conn.cursor()

#     def process_item(self, item, spider):
#         self.store_db(item)
#         #we need to return the item below as Scrapy expects us to!
#         return item

#     def store_in_db(self, item):
#         self.curr.execute(""" insert into db_table values (%s,%s,%s)""", (
#             item["title"][0],
#             item["price"][0],
#             item["url"][0]
#         ))
#         self.conn.commit()