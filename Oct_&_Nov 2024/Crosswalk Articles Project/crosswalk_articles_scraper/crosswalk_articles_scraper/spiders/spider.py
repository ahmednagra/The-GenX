import json
import os
import re
from collections import OrderedDict
from datetime import datetime

import unicodedata
from scrapy import Spider, Request, signals


class CrosswalkSpider(Spider):
    name = "crosswalk"
    allowed_domains = ["www.crosswalk.com"]
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        # "CONCURRENT_REQUESTS": 2,
        "RETRY_TIMES": 7,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 400, 403, 404, 408],
        "FEEDS": {
            f"output/crosswalk Articles Detail {current_dt}.csv": {
                # f"output/crosswalk Articles Detail.csv": {
                "format": "csv",
                "fields": [
                    "filename",
                    "article_title",
                    "article_image",
                    "article_url",
                    "publication_date",
                    "author",
                    "publisher",
                    "publisher_hierarchy",
                    "type",
                    "Category",
                ],
            }
        },
    }

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6",
        "referer": "https://www.crosswalk.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    }

    def __init__(self):
        super().__init__()
        self.items_scraped = 0
        self.crosswalk_section_urls = [
            {
                "faith_spiritual life": "https://www.crosswalk.com/faith/spiritual-life/archives.html"
            },
            {
                "faith_bible study": "https://www.crosswalk.com/faith/bible-study/archives.html"
            },
            {"faith_prayer": "https://www.crosswalk.com/faith/prayer/archives.html"},
            {"faith_women": "https://www.crosswalk.com/faith/women/archives.html"},
            {"faith_men": "https://www.crosswalk.com/faith/men/archives.html"},
            {"faith_seniors": "https://www.crosswalk.com/faith/seniors/archives.html"},
            {
                "family_marriage": "https://www.crosswalk.com/family/marriage/archives.html"
            },
            {
                "family_divorce and remarriage": "https://www.crosswalk.com/family/marriage/divorce-and-remarriage/archives.html"
            },
            {
                "family_singles": "https://www.crosswalk.com/family/singles/archives.html"
            },
            {
                "family_parenting": "https://www.crosswalk.com/family/parenting/archives.html"
            },
            {
                "family_grandparenting": "https://www.crosswalk.com/family/parenting/grandparenting/archives.html"
            },
            {
                "family_homeschool": "https://www.crosswalk.com/family/homeschool/archives.html"
            },
            {"family_career": "https://www.crosswalk.com/family/career/archives.html"},
            {
                "family_finances": "https://www.crosswalk.com/family/finances/archives.html"
            },
            {
                "church_pastors/leadership": "https://www.crosswalk.com/family/finances/archives.html"
            },
            {
                "church_christianity q&a": "https://www.crosswalk.com/church/pastors-or-leadership/christianity-questions-answers/archives.html"
            },
            {
                "church_worship": "https://www.crosswalk.com/church/worship/archives.html"
            },
        ]

        # Logs
        os.makedirs("logs", exist_ok=True)
        self.logs_filepath = f"logs/Crosswalk Logs {self.current_dt}.txt"
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f"[INIT] Script started at {self.script_starting_datetime}")
        self.write_logs(
            f"[INIT] Total Section Scraping: {len(self.crosswalk_section_urls)}"
        )

    def parse_articles(self, response, **kwargs):
        articles_urls = response.css(".archives li a.title::attr(href)").getall()

        self.write_logs(f"{len(articles_urls)} Total Articles Found :{response.url}")

        for article_url in articles_urls:
            yield Request(
                url=article_url,
                callback=self.parse_article_details,
                meta=response.meta,
                headers=self.headers,
            )

        next_page = (
            response.css(".pagination li:last-child a::attr(href)").get("").strip()
        )

        # Check if the URL has http and :80, then correct it to https without the port
        if next_page.startswith("http://") and ":80" in next_page:
            next_page = next_page.replace("http://", "https://").replace(":80", "")

        if next_page:
            self.write_logs(f"Next Page Url: {next_page}")
            yield Request(
                url=next_page,
                callback=self.parse_articles,
                meta=response.meta,
                headers=self.headers,
            )

    def parse_article_details(self, response):
        if response.status != 200:
            self.write_logs(f"Response status code not 200 : URL{response.url}")
            return

        try:
            item = OrderedDict()
            title = response.css('[property="og:title"] ::attr(content)').get("")

            if not title:
                self.write_logs(
                    f"Response is not appropriate format : Url:{response.url}"
                )
                return

            item["filename"] = f"crosswalk_{title}" if title else ""
            item["article_title"] = title
            item["article_image"] = response.css(
                '[property="og:image"] ::attr(content)'
            ).get("")
            item["article_url"] = response.url
            item["publication_date"] = response.css(
                ".articleDescription li:last-child .name::text"
            ).get("")
            item["author"] = response.css(".author-anchor::text").get(
                ""
            ) or response.css(".articleDescription li .name::text").get("")
            item["publisher"] = "Crosswalk"
            item["publisher_hierarchy"] = "Salem"
            item["type"] = "Website"

            # test
            print(f'Author: {item["author"] }')
            # Write the article body text to a file
            self.write_txt_file(item["filename"], response)

            self.items_scraped += 1
            print(f"Items Are scraped: {self.items_scraped}")
            yield item
        except Exception as e:
            self.write_logs(
                f"Error processing article details for URL: {response.url}. Error: {e}"
            )

    def write_txt_file(self, filename, response):
        output_folder = "output"
        try:
            # Check if the output folder exists, create it if not
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
                self.write_logs(f"Created output folder: {output_folder}")

            # Clean and validate the filename
            cleaned_filename = re.sub(r'[<>:"/\\|?*]', "", filename)
            if not cleaned_filename:
                self.write_logs(f"Invalid filename after cleaning: {filename}")
                return

            article_body = response.css(".articleContentBody > *")

            content = []
            for tag in article_body:
                if tag.root.tag == "div":  # Skip if the tag is a <div>
                    continue

                # Check if 'Photo Credit' is in the text and break if found
                if "Photo Credit" in tag.css("::text").get(""):
                    print("photo Credit in text now break")
                    break

                # Append text content
                content.append(tag.css("::text").getall())

            # Flatten content list to get all text as a single string
            text1 = "\n".join(["".join(texts) for texts in content])

            if not text1:
                script_tag = [
                    script
                    for script in response.css(
                        'script[type="application/ld+json"]::text'
                    ).getall()
                    if '"@type": "Article"' in script
                ][0].strip()
                clean_script = re.sub(r"[\x00-\x1F\x7F]", "", script_tag)
                data_dict = json.loads(clean_script)

                # Get the article text, stopping at 'Photo Credit'
                text1 = (
                    data_dict.get("description", "").split("Photo Credit")[0]
                    if data_dict
                    else ""
                )

            # Normalize unicode characters to closest ASCII equivalents
            normalized_text = unicodedata.normalize("NFKD", text1)

            # Encode to ASCII and ignore non-ASCII characters
            text = normalized_text.encode("ascii", "ignore").decode("utf-8")

            # Write the text to the specified file
            file_path = os.path.join(output_folder, f"{cleaned_filename}.txt")
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(text)

            self.write_logs(f"Successfully wrote to file: {file_path}")
        except Exception as e:
            self.write_logs(
                f"ile Name: {filename}  An error occurred while getting information: {e}"
            )

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode="a", encoding="utf-8") as logs_file:
            logs_file.write(f"{log_msg}\n")
            print(log_msg)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CrosswalkSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        if self.crosswalk_section_urls:
            section = self.crosswalk_section_urls.pop(0)
            key, value = next(iter(section.items()))
            self.write_logs(
                f"Section :{key.split('_')[1]} Is called for Articles Scraping"
            )
            req = Request(
                url=value,
                callback=self.parse_articles,
                dont_filter=True,
                headers=self.headers,
                meta={"handle_httpstatus_all": True, "section": key},
            )

            try:
                self.crawler.engine.crawl(req)  # For latest Python version
            except TypeError:
                self.crawler.engine.crawl(req, self)  # For old Python version < 10

    def close(spider, reason):
        spider.write_logs(f"Total Articles are Scraped: {spider.items_scraped}")
        spider.write_logs(f"Script Started at: {spider.script_starting_datetime}")
        spider.write_logs(
            f'Script Stopped at: {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}'
        )
