import json
from bs4 import BeautifulSoup
import re
import scrapy
from urllib.parse import urlparse, parse_qs
import csv
from markdownify import markdownify
import os


class OliveyoungSpiderSpider(scrapy.Spider):
    name = "oliveyoung_spider"
    PROJECT_PATH = os.path.join(os.path.dirname(__file__), './')

    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': PROJECT_PATH + 'output/GA230819992.csv',  # Change to CSV format
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'IMAGES_STORE': os.path.join(PROJECT_PATH, 'output/images'),
        'ITEM_PIPELINES': {
            'oliveyoung.pipelines.ProductImagesPipeline': 1,  # Add the pipeline here
        }
    }

    def __init__(self, *args, **kwargs):
        super(OliveyoungSpiderSpider, self).__init__(*args, **kwargs)
        # Create output and images directories
        output_dir = os.path.join(self.PROJECT_PATH, 'output')
        images_dir = os.path.join(output_dir, 'images')
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(images_dir, exist_ok=True)
        self.output_dir = output_dir

    def start_requests(self):
        url = 'https://global.oliveyoung.com/product/detail?prdtNo=GA230819992'
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        url = response.url
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)

        # Extract prdtNo
        prdt_no = str(params.get('prdtNo', [None])[0])
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            # 'cookie': 'awsCntryCode=10; acesCntry=00; dlvCntry=10; currency=USD; curLang=en; lang=en; FOSID=Y2NkMTgwZTctOGQyZi00M2YyLThkZGYtOWQwYWI0MTI4MDRj; _gcl_au=1.1.1419031249.1729255742; RECENT_VIEW_PRODUCT=%5B%7B%22prdtNo%22%3A%22GA230819992%22%2C%22imagePath%22%3A%22https%3A%2F%2Fimage.globaloliveyoungshop.com%2FprdtImg%2F1990%2F9b54f017-16da-43c7-b614-8e86d215d0dd.jpg%3FSF%3Dwebp%26QT%3D80%22%2C%22imageName%22%3A%22AROMATICA%20Rosemary%20Root%20Enhancer%20100mL%2B100mL%20Double%20Set%22%7D%5D; _ga=GA1.2.1621401730.1729255745; _gid=GA1.2.541820911.1729255746; _scid=w91j7MHETjNoY0f1RNPUB8riL2Ztrn_l; _scid_r=w91j7MHETjNoY0f1RNPUB8riL2Ztrn_l; _fbp=fb.1.1729255763666.41595509979587348; _ScCbts=%5B%2257%3Bchrome.2%3A2%3A5%22%5D; _tt_enable_cookie=1; _ttp=eCP7aiCHgVZjYVjdQxrde9NDsXn; _sctr=1%7C1729191600000; _ga_5ZDXC4W9LE=GS1.1.1729255744.1.1.1729256449.60.0.0; _dd_s=rum=0&expire=1729257351727',
            'origin': 'https://global.oliveyoung.com',
            'priority': 'u=1, i',
            'referer': 'https://global.oliveyoung.com/product/detail?prdtNo=GA230819992',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        json_data = {
            'prdtNo': prdt_no,
        }
        yield scrapy.Request(url='https://global.oliveyoung.com/product/detail-data', body=json.dumps(json_data),
                             callback=self.detail_data, headers=headers, method='POST', meta={'prdt_no': prdt_no})

    def detail_data(self, response):
        product_info = {}
        prdt_no = response.meta.get('prdt_no')
        data = json.loads(response.text)
        product = data.get('product')
        product_info['product_name'] = product.get('prdtName')
        product_info['price_usd'] = product.get('nrmlAmt')
        product_info['discounted_price_usd'] = product.get('saleAmt')
        product_info['brand'] = product.get('brandName')
        prdtGbnCode = product.get('prdtGbnCode')
        thumbnailList = product.get('thumbnailList', [])
        product_images = []
        for thumbnail in thumbnailList:
            thumbnail = thumbnail.get('imagePath')
            if thumbnail:
                thumbnail = 'https://image.globaloliveyoungshop.com/' + thumbnail
                product_images.append(thumbnail)
        product_info['product_images'] = product_images
        json_data = {
            'prdtNo': prdt_no,
        }
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            # 'cookie': 'awsCntryCode=10; acesCntry=00; dlvCntry=10; currency=USD; curLang=en; lang=en; FOSID=Y2NkMTgwZTctOGQyZi00M2YyLThkZGYtOWQwYWI0MTI4MDRj; _gcl_au=1.1.1419031249.1729255742; RECENT_VIEW_PRODUCT=%5B%7B%22prdtNo%22%3A%22GA230819992%22%2C%22imagePath%22%3A%22https%3A%2F%2Fimage.globaloliveyoungshop.com%2FprdtImg%2F1990%2F9b54f017-16da-43c7-b614-8e86d215d0dd.jpg%3FSF%3Dwebp%26QT%3D80%22%2C%22imageName%22%3A%22AROMATICA%20Rosemary%20Root%20Enhancer%20100mL%2B100mL%20Double%20Set%22%7D%5D; _ga=GA1.2.1621401730.1729255745; _gid=GA1.2.541820911.1729255746; _scid=w91j7MHETjNoY0f1RNPUB8riL2Ztrn_l; _scid_r=w91j7MHETjNoY0f1RNPUB8riL2Ztrn_l; _fbp=fb.1.1729255763666.41595509979587348; _ScCbts=%5B%2257%3Bchrome.2%3A2%3A5%22%5D; _tt_enable_cookie=1; _ttp=eCP7aiCHgVZjYVjdQxrde9NDsXn; _sctr=1%7C1729191600000; _ga_5ZDXC4W9LE=GS1.1.1729255744.1.1.1729256449.60.0.0; _dd_s=rum=0&expire=1729257351727',
            'origin': 'https://global.oliveyoung.com',
            'priority': 'u=1, i',
            'referer': 'https://global.oliveyoung.com/product/detail?prdtNo=GA230819992',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        yield scrapy.Request(url='https://global.oliveyoung.com/product/description-info', headers=headers, body=json.dumps(json_data),
                             callback=self.description_info, method='POST', meta={'prdt_no': prdt_no,'prdtGbnCode':prdtGbnCode,
                                                                                  'product_info': product_info})

    def description_info(self, response):
        prdtGbnCode = response.meta.get('prdtGbnCode')
        product_info = response.meta.get('product_info')
        prdt_no = response.meta.get('prdt_no')
        data = json.loads(response.text)
        description = data.get('description', [])
        for item in description:
            if item.get('codeDtlName') == 'Content volume or weight':
                product_info['specific_item_info_content_volume_or_weight_ml'] = item.get('itemCont')
            if item.get('codeDtlName') == 'Ideal for':
                product_info['specific_item_info_ideal_for'] = item.get('itemCont')
            if item.get('codeDtlName') == 'Expiration date (or expiration date after opening)':
                product_info['specific_item_info_expiration_date'] = item.get('itemCont')
            if item.get('codeDtlName') == 'How to Use':
                product_info['specific_item_info_how_to_use'] = item.get('itemCont')
            if item.get('codeDtlName') == 'Cosmetics manufacturers, cosmetics responsible distributors, and customized cosmetics sellers':
                product_info['specific_item_info_cosmetics_manufacturers'] = item.get('itemCont')
            if item.get('codeDtlName') == 'Country of Manufacture':
                product_info['specific_item_info_country_of_manufacture'] = item.get('itemCont')
            if item.get('codeDtlName') == 'Ingredients':
                product_info['specific_item_info_ingredients'] = item.get('itemCont')
            if item.get('codeDtlName') == 'MFDS Evaluation of Functional Cosmetics':
                product_info['specific_item_info_mfds_evaluation_of_functional_cosmetics'] = item.get('itemCont')
            if item.get('codeDtlName') == 'Precautions when using':
                product_info['specific_item_info_precautions'] = item.get('itemCont')
        json_data = {
            'prdtNo': prdt_no,
        }
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            # 'cookie': 'awsCntryCode=10; acesCntry=00; dlvCntry=10; currency=USD; curLang=en; lang=en; FOSID=Y2NkMTgwZTctOGQyZi00M2YyLThkZGYtOWQwYWI0MTI4MDRj; _gcl_au=1.1.1419031249.1729255742; RECENT_VIEW_PRODUCT=%5B%7B%22prdtNo%22%3A%22GA230819992%22%2C%22imagePath%22%3A%22https%3A%2F%2Fimage.globaloliveyoungshop.com%2FprdtImg%2F1990%2F9b54f017-16da-43c7-b614-8e86d215d0dd.jpg%3FSF%3Dwebp%26QT%3D80%22%2C%22imageName%22%3A%22AROMATICA%20Rosemary%20Root%20Enhancer%20100mL%2B100mL%20Double%20Set%22%7D%5D; _ga=GA1.2.1621401730.1729255745; _gid=GA1.2.541820911.1729255746; _scid=w91j7MHETjNoY0f1RNPUB8riL2Ztrn_l; _scid_r=w91j7MHETjNoY0f1RNPUB8riL2Ztrn_l; _fbp=fb.1.1729255763666.41595509979587348; _ScCbts=%5B%2257%3Bchrome.2%3A2%3A5%22%5D; _tt_enable_cookie=1; _ttp=eCP7aiCHgVZjYVjdQxrde9NDsXn; _sctr=1%7C1729191600000; _ga_5ZDXC4W9LE=GS1.1.1729255744.1.1.1729256449.60.0.0; _dd_s=rum=0&expire=1729257351727',
            'origin': 'https://global.oliveyoung.com',
            'priority': 'u=1, i',
            'referer': 'https://global.oliveyoung.com/product/detail?prdtNo=GA230819992',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        yield scrapy.Request(url='https://global.oliveyoung.com/product/details-info', headers=headers,
                             body=json.dumps(json_data),
                             callback=self.detail_info, method='POST', meta={'prdt_no': prdt_no,'prdtGbnCode':prdtGbnCode,
                                                                             'product_info': product_info})

    def detail_info(self, response):
        prdtGbnCode = response.meta.get('prdtGbnCode')
        prdt_no = response.meta.get('prdt_no')
        product_info = response.meta.get('product_info')
        data = json.loads(response.text)
        product_info['product_info_why_we_love_it'] = data.get('details', {}).get('whyWeLoveItText')
        product_info['product_info_featured_ingredients'] = data.get('details', {}).get('ftrdIngrdText')
        product_info['product_info_how_to_use'] = data.get('details', {}).get('howToUseText')

        # Assuming brochure_text contains the HTML content
        brochure_text = data.get('details', {}).get('dtlDesc')

        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(brochure_text, 'html.parser')

        # Remove all div tags completely, even if they're incomplete or broken
        for div in soup.find_all('div'):
            div.decompose()

        # Re-check for any incomplete divs or remnants by cleaning using regex
        cleaned_html = str(soup)
        cleaned_html = re.sub(r'<div[^>]*>', '', cleaned_html)  # Remove any remaining div tags

        # Extract only the text content, stripping out all other tags and styles
        clean_text = BeautifulSoup(cleaned_html, 'html.parser').get_text()

        # Remove URLs (including image links) using regex
        clean_text = re.sub(r'http\S+|www\S+|https\S+', '', clean_text)

        # Remove unnecessary extra spaces and newlines
        clean_text = re.sub(r'\n+', '\n', clean_text).strip()

        # Optionally convert the cleaned text to markdown (if required)
        markdown_text = markdownify(clean_text)

        # Print the cleaned and formatted text without tags, URLs, and extra spaces
        product_info['brochure_text'] = markdown_text.replace('![]( /></figure>', '').replace('<div class=)', '')
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            # 'cookie': 'awsCntryCode=10; acesCntry=00; dlvCntry=10; currency=USD; curLang=en; lang=en; FOSID=Y2NkMTgwZTctOGQyZi00M2YyLThkZGYtOWQwYWI0MTI4MDRj; _gcl_au=1.1.1419031249.1729255742; RECENT_VIEW_PRODUCT=%5B%7B%22prdtNo%22%3A%22GA230819992%22%2C%22imagePath%22%3A%22https%3A%2F%2Fimage.globaloliveyoungshop.com%2FprdtImg%2F1990%2F9b54f017-16da-43c7-b614-8e86d215d0dd.jpg%3FSF%3Dwebp%26QT%3D80%22%2C%22imageName%22%3A%22AROMATICA%20Rosemary%20Root%20Enhancer%20100mL%2B100mL%20Double%20Set%22%7D%5D; _gid=GA1.2.541820911.1729255746; _scid=w91j7MHETjNoY0f1RNPUB8riL2Ztrn_l; _scid_r=w91j7MHETjNoY0f1RNPUB8riL2Ztrn_l; _fbp=fb.1.1729255763666.41595509979587348; _ScCbts=%5B%2257%3Bchrome.2%3A2%3A5%22%5D; _tt_enable_cookie=1; _ttp=eCP7aiCHgVZjYVjdQxrde9NDsXn; _sctr=1%7C1729191600000; _dd_s=rum=0&expire=1729257351727; _ga=GA1.1.1621401730.1729255745; _ga_5ZDXC4W9LE=GS1.1.1729255744.1.1.1729256452.57.0.0',
            'origin': 'https://global.oliveyoung.com',
            'priority': 'u=1, i',
            'referer': 'https://global.oliveyoung.com/product/detail?prdtNo=GA230819992',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        json_data = {
            'filterYn': 'N',
            'prdtNo': prdt_no,
            'prdtGbnCode': prdtGbnCode,
            'movReviewYn': 'N',
            'photoReviewYn': 'N',
            'optnYn': 'N',
            'transUseYn': 'N',
            'filterUseYn': 'Y',
        }
        yield scrapy.Request(url='https://global.oliveyoung.com/product/review-summary', headers=headers,
                             body=json.dumps(json_data),
                             callback=self.review_summary, method='POST', meta={'prdt_no': prdt_no,'prdtGbnCode':prdtGbnCode,
                                                                                'product_info': product_info})

    def review_summary(self, response):
        prdtGbnCode = response.meta.get('prdtGbnCode')
        product_info = response.meta.get('product_info')
        prdt_no = response.meta.get('prdt_no')
        data = json.loads(response.text)
        totalReviewCount = data.get('totalReviewCount')
        product_info['reviews_photo_and_video_number'] = data.get('totalMediaReviewCount')
        product_info['reviews_rating'] = data.get('totalStarRate')
        product_info['reviews_number'] = data.get('totalReviewCount')
        scores = data.get('scores', [])
        for score in scores:
            if score.get('star') == 5:
                product_info['reviews_5_star_perc'] = score.get('rate')
            if score.get('star') == 4:
                product_info['reviews_4_star_perc'] = score.get('rate')
            if score.get('star') == 3:
                product_info['reviews_3_star_perc'] = score.get('rate')
            if score.get('star') == 2:
                product_info['reviews_2_star_perc'] = score.get('rate')
            if score.get('star') == 1:
                product_info['reviews_1_star_perc'] = score.get('rate')
        evltScores = data.get('evltScores', [])
        for evltScore in evltScores:
            if evltScore.get('evltItemNm') == 'Softens the hair':
                product_info['reviews_softens_the_hair_stars'] = evltScore.get('rate')
            if evltScore.get('evltItemNm') == 'Scent':
                product_info['reviews_scent_stars'] = evltScore.get('rate')
            if evltScore.get('evltItemNm') == 'Formulation':
                product_info['reviews_formulation_stars'] = evltScore.get('rate')
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            # 'cookie': 'awsCntryCode=10; acesCntry=00; dlvCntry=10; currency=USD; curLang=en; lang=en; FOSID=Y2NkMTgwZTctOGQyZi00M2YyLThkZGYtOWQwYWI0MTI4MDRj; _gcl_au=1.1.1419031249.1729255742; RECENT_VIEW_PRODUCT=%5B%7B%22prdtNo%22%3A%22GA230819992%22%2C%22imagePath%22%3A%22https%3A%2F%2Fimage.globaloliveyoungshop.com%2FprdtImg%2F1990%2F9b54f017-16da-43c7-b614-8e86d215d0dd.jpg%3FSF%3Dwebp%26QT%3D80%22%2C%22imageName%22%3A%22AROMATICA%20Rosemary%20Root%20Enhancer%20100mL%2B100mL%20Double%20Set%22%7D%5D; _gid=GA1.2.541820911.1729255746; _scid=w91j7MHETjNoY0f1RNPUB8riL2Ztrn_l; _fbp=fb.1.1729255763666.41595509979587348; _ScCbts=%5B%2257%3Bchrome.2%3A2%3A5%22%5D; _tt_enable_cookie=1; _ttp=eCP7aiCHgVZjYVjdQxrde9NDsXn; _sctr=1%7C1729191600000; _scid_r=3F1j7MHETjNoY0f1RNPUB8riL2Ztrn_lD8vhRg; _ga=GA1.2.1621401730.1729255745; _ga_5ZDXC4W9LE=GS1.1.1729261498.3.0.1729261498.60.0.0; _dd_s=rum=0&expire=1729265292775; _gat_UA-141211198-4=1; _gat_UA-141211198-1=1',
            'origin': 'https://global.oliveyoung.com',
            'priority': 'u=1, i',
            'referer': 'https://global.oliveyoung.com/product/detail?prdtNo=GA230819992',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        json_data = {
            'filterYn': 'N',
            'prdtNo': prdt_no,
            'prdtGbnCode': prdtGbnCode,
            'movReviewYn': 'N',
            'photoReviewYn': 'N',
            'optnYn': 'N',
            'transUseYn': 'N',
            'pageNum': 1,
            'rowsPerPage': totalReviewCount,
            'sort': '10',
        }
        yield scrapy.Request(url='https://global.oliveyoung.com/product/review-list', headers=headers,
                             body=json.dumps(json_data),
                             callback=self.review_list, method='POST', meta={'prdt_no': prdt_no,
                                                                             'product_info': product_info})
    def review_list(self, response):
        prdt_no = response.meta.get('prdt_no')
        product_info = response.meta.get('product_info')
        data = json.loads(response.text)
        reviewList = data.get('reviewList', [])
        csv_file = os.path.join(self.output_dir, 'reviews.csv')

        # Define the field names (header) for the CSV
        fieldnames = ['product_code', 'date', 'general_stars', 'softens_the_hair_stars', 'formulation_stars',
                      'scent_stars', 'key_words', 'text', 'number_of_likes']

        # Open the CSV file in append mode ('a'), and ensure newline='' to avoid blank lines
        with open(csv_file, mode='a', newline='', encoding='utf-8-sig') as file:
            # Create a DictWriter object, passing the fieldnames
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            # Check if the file is empty and write the header if necessary
            if file.tell() == 0:
                writer.writeheader()  # Write the header (column names) only if file is new/empty
            for review in reviewList:
                date = review.get('reviewRgstYmd')
                general_stars = review.get('previewScore')
                reviewEvltList = review.get('reviewEvltList', [])
                for reviewEvlt in reviewEvltList:
                    if reviewEvlt.get('evltItemNm') == 'Softens the hair':
                        softens_the_hair_stars = reviewEvlt.get('previewScore')
                    if reviewEvlt.get('evltItemNm') == 'Formulation':
                        formulation_stars = reviewEvlt.get('previewScore')
                    if reviewEvlt.get('evltItemNm') == 'Scent':
                        scent_stars = reviewEvlt.get('previewScore')
                key_words = []
                reviewPrflList = review.get('reviewPrflList', [])
                for reviewPrf in reviewPrflList:
                    if reviewPrf.get('prflClassNm') == 'Hair Type':
                        value = f"Hair Type: {reviewPrf.get('prflItemNms')}"
                        key_words.append(value)
                    if reviewPrf.get('prflClassNm') == 'Hair Concern':
                        value = f"Hair Concern: {reviewPrf.get('prflItemNms')}"
                        key_words.append(value)
                text = review.get('conText')
                number_of_likes = review.get('goodCnt')

                # Example scraped data
                scraped_data = {
                    'product_code': prdt_no,
                    'date': date,
                    'general_stars': general_stars,
                    'softens_the_hair_stars': softens_the_hair_stars,
                    'formulation_stars': formulation_stars,
                    'scent_stars': scent_stars,
                    'key_words': '\n'.join(key_words),
                    'text': text,
                    'number_of_likes': number_of_likes
                }

                # Write the scraped data as a row in the CSV
                writer.writerow(scraped_data)

        yield product_info




