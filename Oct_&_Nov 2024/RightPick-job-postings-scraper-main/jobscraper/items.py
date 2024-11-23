# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class JobItem(scrapy.Item):
    company = scrapy.Field()
    date = scrapy.Field()
    id_unique = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    location = scrapy.Field()
    description = scrapy.Field()
    job_id = scrapy.Field()
    url_id = scrapy.Field()
    job_skill_group = scrapy.Field()
    job_skill_code = scrapy.Field()
    interest = scrapy.Field()
    interest_category = scrapy.Field()
    functions = scrapy.Field()
    industries = scrapy.Field()
    who_you_will_work_with = scrapy.Field()
    what_you_will_do = scrapy.Field()
    your_background = scrapy.Field()
    post_to_linkedin = scrapy.Field()
    job_apply_url = scrapy.Field()
    salary = scrapy.Field()
    benefits = scrapy.Field()
    requirements = scrapy.Field()
    responsibilities = scrapy.Field()
    industry = scrapy.Field()
    seniority = scrapy.Field()

