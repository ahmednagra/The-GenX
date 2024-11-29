import json
import re
from collections import OrderedDict
from datetime import datetime
from urllib.parse import urljoin, quote

from scrapy import Spider, Request


class RetailerSpider(Spider):
    name = "amazon"

    custom_settings = {
        'CONCURRENT_REQUESTS': 2,
        'RETRY_TIMES': 7,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],

        'FEEDS': {
            f'output/Amazon Products Detail {datetime.now().strftime("%d%m%Y%H%M")}.csv': {
                'format': 'csv',
                'fields': ['Product URL', 'Brand Name', 'Product Name', 'Special Price', 'Regular Price', 'Sold By',
                           'Shipped From', 'Shipping Cost', 'Short Description',
                           'Long Description', 'Product Information', 'Directions', 'Ingredients', 'SKU', 'ASIN',
                           'Barcode', 'Image URLs']
            }
        }
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'max-age=0',
        # 'cookie': 'session-id=258-5635801-5883041; session-id-time=2082787201l; i18n-prefs=GBP; ubid-acbuk=259-4005184-1934553; session-token="dN3jo1+YqJk1zo7XaQuS7/pKl1o2xaoNjOZl8k6f8qT9VPAiQhfge48yOH9pem8QvYe557SfBt6AF/PtvhL9Cu98RUJBTYINHb4/Ya+t/egY2fIbhVWXd2GMTSD720UcSqnHk71Sn2e44YLeKZfdmP4iz4ZAuXaKEBoWhw5bBN2K/2Nov2+CYS2kTIDPfcVTWErOqDhq9DRVHwO76sUZY25+LLgOg3KRXG/R4PNPcYvEpDXv2uTj1cT2sINTr4mt90tiMvce7loKxFNK0EgeXNu3FvuJlv0dHLtamr3eAdmDjlIJCk6v5KjQ7Mp0ELSwn8z4KZmO46YZAfA/XJzxS9L9TGTk4p82Xy6JZ9ot6Vw="; csm-hit=tb:E5XVZ0RJBFPNQJVVK5VG+s-WNC1VJ1KWYJF0TKNV6TR|1730299719476&t:1730299719476&adb:adblk_yes',
        'device-memory': '8',
        'downlink': '1.5',
        'dpr': '1.25',
        'ect': '3g',
        'priority': 'u=0, i',
        'referer': 'https://www.amazon.co.uk/Best-Sellers-Computers-Accessories-Laptops/zgbs/computers/429886031/ref=zg_m_bs_nav_computers_1',
        'rtt': '400',
        'sec-ch-device-memory': '8',
        'sec-ch-dpr': '1.25',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"15.0.0"',
        'sec-ch-viewport-width': '1536',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'viewport-width': '1536',
    }
    cookies = {
        # 'session-id': '258-5635801-5883041',
        # 'session-id-time': '2082787201l',
        'i18n-prefs': 'GBP',
        'ubid-acbuk': '259-4005184-1934553',
        'lc-acbuk': 'en_GB',
        # 'session-token': '"dN3jo1+YqJk1zo7XaQuS7/pKl1o2xaoNjOZl8k6f8qT9VPAiQhfge48yOH9pem8QvYe557SfBt6AF/PtvhL9Cu98RUJBTYINHb4/Ya+t/egY2fIbhVWXd2GMTSD720UcSqnHk71Sn2e44YLeKZfdmP4iz4ZAuXaKEBoWhw5bBN2K/2Nov2+CYS2kTIDPfcVTWErOqDhq9DRVHwO76sUZY25+LLgOg3KRXG/R4PNPcYvEpDXv2uTj1cT2sINTr4mt90tiMvce7loKxFNK0EgeXNu3FvuJlv0dHLtamr3eAdmDjlIJCk6v5KjQ7Mp0ELSwn8z4KZmO46YZAfA/XJzxS9L9TGTk4p82Xy6JZ9ot6Vw="',
        # 'csm-hit': 'tb:E5XVZ0RJBFPNQJVVK5VG+s-WNC1VJ1KWYJF0TKNV6TR|1730299719476&t:1730299719476&adb:adblk_yes',
    }

    def __init__(self):
        super().__init__()

        proxy_file_path = 'input/proxy_key.txt'
        urls_file_path = 'input/urls.txt'
        self.urls = self.get_input_rows_from_file(urls_file_path)
        proxy_token = self.get_input_rows_from_file(proxy_file_path)
        cookies = quote('i18n-prefs=GBP; ubid-acbuk=259-4005184-1934553; lc-acbuk=en_GB') # Cookies are set for London Uk Address
        self.proxy = f"http://{proxy_token[0]}:setCookies={cookies}@proxy.scrape.do:8080"

        # self.proxy = f"http://cef2263bc9d547608ce8aab5fd735feb1d5c2170fa8:setCookies={cookies}@proxy.scrape.do:8080"
        a=1

    def start_requests(self):
        for url in self.urls:
            yield Request(url=url, callback=self.parse_categories, meta={'page': 'Indexing Page'})
            # yield Request(url=url, callback=self.parse_categories, headers=self.headers,
            #               cookies=self.cookies, meta={'page': 'Indexing Page'})

    def parse_categories(self, response, **kwargs):
        if response.meta.get('page') == 'Indexing Page':
            yield from self.parse_products(response)

        # cookies = {cookie.split(b'=')[0].decode(): cookie.split(b'=')[1].split(b';')[0].decode() for cookie in
        #            response.headers.getlist('Set-Cookie')}
        #
        # cook = cookies if cookies else self.cookies
        # Get all the categories, sub categories sub sub categories
        category_urls = self.get_sub_category_urls(response)
        print(f'Category exists: {len(category_urls)}')

        for url in category_urls:
            url = urljoin(response.url, url)
            # self.headers['referer'] = url
            print(f'Sub Category URl:{url}')
            yield Request(url=url, callback=self.parse_products)

        if not category_urls:
            print('No more Cate Category')
            yield from self.parse_products(response)  # To parse products in the given category itself even if it has sub cats

    def parse_products(self, response):
        # cookies = {cookie.split(b'=')[0].decode(): cookie.split(b'=')[1].split(b';')[0].decode() for cookie in
        #            response.headers.getlist('Set-Cookie')}
        #
        # cook = cookies if cookies else self.cookies
        # self.headers['referer'] = response.url
        products_urls = list(set(response.css('#gridItemRoot a[role="link"]::attr(href)').getall()))

        if products_urls:
            for product_url in products_urls[:2]:
                url = urljoin(response.url, product_url)

                print(f'Product URl:{url}')
                yield Request(url=url, callback=self.parse_details)

        next_page_urlaaaa = self.get_next_page_url(response)
        next_page_url = response.css('.a-pagination .a-last a::attr(href)').get('')

        if next_page_url:
            url = urljoin(response.url, next_page_url)
            yield Request(url=url, callback=self.parse_products)

    def parse_details(self, response):
        try:
            item = OrderedDict()

            current_price = self.get_discounted_price(response)
            was_price = self.get_regular_price(response)

            item['Product URL'] = response.url
            item['Brand Name'] = self.get_brand_name(response)
            item['Product Name'] = response.css('#productTitle::text').get('').strip()

            if was_price:
                item['Special Price'] = current_price
                item['Regular Price'] = was_price
            else:
                item['Regular Price'] = current_price if current_price else 'Out of Stock'

            item['Sold By'] = self.get_seller_name(response)
            item['Shipped From'] = self.get_shipped_from(response)
            item['Shipping Cost'] = self.get_shipping_cost(response)
            item['Short Description'] = response.css('#feature-bullets .a-list-item::text').getall() or ''
            item['Long Description'] = response.css('#productDescription span::text').getall() or ''
            item['Product Information'] = self.get_product_information(response)
            item['Directions'] = response.css(
                '#important-information div.content:nth-child(3) p:nth-child(2)::text').get(
                '')
            item['Ingredients'] = response.css(
                '#important-information div.content:nth-child(2) p:nth-child(2)::text').get(
                '') or response.css('.a-section.content:nth-child(2) p::text').getall()
            asin = item.get('Product Information', {}).get('ASIN', '') or \
                   item['Product URL'].replace('?th=1', '').split("/dp/")[1].split("/")[0] if item.get('Product URL',
                                                                                                       '').replace(
                '?th=1', '').count('/dp/') == 1 else None
            item['SKU'] = asin
            item['ASIN'] = asin
            item['Barcode'] = ''

            try:
                images_json = json.loads(
                    response.css('script[type="text/javascript"]:contains(ImageBlockATF)').re_first(
                        f"'colorImages':(.*)").rstrip(',').replace("'", '"')) or {}
                images_json = images_json.get('initial', [])
            except json.JSONDecodeError:
                images_json = []
            except AttributeError:
                images_json = []

            full_size_images_url = [item.get('hiRes', '') for item in images_json]
            images = [url for url in
                      response.css('.regularAltImageViewLayout .a-list-item .a-button-text img::attr(src)').getall() if
                      'images-na.ssl' not in url] or []

            images_url = [re.sub(r'\._.*', '._AC_SX522_.jpg', url) for url in images]

            item['Image URLs'] = full_size_images_url or images_url

            see_all_options = response.css('#buybox-see-all-buying-choices a::attr(href)')

            # if not current_price and see_all_options:
            #     url = f'https://www.amazon.com.au/gp/product/ajax/ref=dp_aod_unknown_mbc?asin={asin}&m=&qid=&smid=&sourcecustomerorglistid=&sourcecustomerorglistitemid=&sr=&pc=dp&experienceId=aodAjaxMain'
            #     yield Request(url=url,
            #                   meta={'handle_httpstatus_all': True},
            #                   callback=self.get_process_price, cb_kwargs={'item': item})
            # else:
            yield item
        except Exception as e:
            a=1

    def get_input_rows_from_file(self, file_path):
        try:
            with open(file_path, mode='r') as txt_file:
                return [line.strip() for line in txt_file.readlines() if line.strip()]

        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return []
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return []

    def get_sub_category_urls(self, response):
        # bestseller category
        child_category = response.css(
            'div:has(span._p13n-zg-nav-tree-all_style_zg-selected__1SfhQ) ~ div[role="group"]')
        category_urls = response.css(
            'div[role="group"] div[role="treeitem"] a::attr(href)').getall() if child_category else []  # best seller categories url
        category_urls = category_urls or response.css(
            'ul > li > span > a.a-color-base.a-link-normal::attr(href)').getall()
        category_urls = category_urls or response.css(
            '.a-spacing-micro.s-navigation-indent-2 a::attr(href)').getall()

        return category_urls

    def get_product_urls(self, response):
        bestseller_tag = response.css('#gridItemRoot a:nth-child(2)::attr(href)')
        products_url = []

        if bestseller_tag:
            json_data = json.loads(response.css('[data-client-recs-list] ::attr(data-client-recs-list)').get(''))
            products_asins = [item['id'] for item in json_data]

            for asin in products_asins:
                url = f'https://www.amazon.com.au/dp/{asin}'
                products_url.append(url)

        products_url = products_url or response.css(
            '.s-line-clamp-2 a::attr(href), .s-line-clamp-4 a::attr(href)').getall()
        products_url = products_url or response.css(
            '.a-size-mini.a-spacing-none.a-color-base.s-line-clamp-4 a::attr(href)').getall()
        products_url = products_url or [response.url]

        return products_url

    def get_next_page_url(self, response):
        next_page = response.css('.s-pagination-selected + a::attr(href)').get('')
        next_page = next_page or response.css('.a-last a::attr(href)').get('')

        return response.urljoin(next_page)

    def get_product_information(self, response):
        product_information = {}

        rows = response.css('#productDetails_techSpec_section_1 tr') or response.css(
            '.content-grid-block table tr') or ''
        if not rows:
            product_details = response.css('#detailBullets_feature_div li')
        else:
            product_details = []

        for row in rows:
            key = row.css('th::text').get('') or row.css('td strong::text').get('')
            value = row.css('td p::text').get('') or row.css('td::text').get('')
            if key and value:
                value = value.replace('\u200e', '')
                value = ' '.join(value.strip().split())
                product_information[key.strip()] = value

        for detail in product_details:
            key = detail.css('.a-text-bold::text').get('')
            value = detail.css('.a-text-bold + span::text').get('')
            if key and value:
                key = key.replace(':', '').replace('\u200e', '').replace(' \u200f', '')
                key = ' '.join(key.strip().split())
                value = value.replace('\u200e', '')
                value = ' '.join(value.strip().split())
                product_information[key] = value

        additional_information = response.css('#productDetails_detailBullets_sections1 tr') or ''

        for row in additional_information:
            key = row.css('th::text').get('')
            value = ' '.join(row.css('td *::text').getall()).strip()
            if key and value:
                value = value.split('\n')[-1].strip()
                product_information[key.strip()] = value

        return product_information

    def get_discounted_price(self, response):
        price = response.css('#attach-base-product-price::attr(value)').get('')
        if price:
            price = f'£{price}'

        price = price or ''.join(response.css('#corePriceDisplay_desktop_feature_div .priceToPay ::text').getall())
        price = price or response.css('.reinventPricePriceToPayMargin .a-offscreen::text').get('').replace('£', '')
        price = price or response.css('.apexPriceToPay span.a-offscreen::text').get('').replace('£', '')

        return price

    def get_regular_price(self, response):
        price = response.css('.aok-relative .a-size-small.aok-offscreen::text').re_first(r'\d+\.?\d*')
        if price:
            price = f'£{price}'
        # price = price or response.css('.a-price[data-a-color="secondary"] ::text').get('')
        # price = price or response.css('.basisPrice .a-offscreen::text').get('').replace('£', '')
        # price = price or ''.join(response.css('#corePriceDisplay_desktop_feature_div .priceToPay ::text').getall())

        return price

    def get_process_price(self, response, item):
        item['Regular Price'] = response.css('.a-price-whole::text').get('')
        item['Sold By'] = response.css('#aod-offer-soldBy a[role="link"]::text').get('')
        item['Shipped From'] = response.css('#aod-offer-shipsFrom .a-color-base::text').get('')
        item['Shipping Cost'] = response.css(
            '#mir-layout-DELIVERY_BLOCK span[data-csa-c-delivery-price]::attr(data-csa-c-delivery-price)').get('')

        yield item

    def get_shipped_from(self, response):
        shipped = response.css('.a-section.show-on-unselected .truncate .a-size-small:nth-child(2)::text').get('')
        shipped = shipped or response.css(
            '.a-section.show-on-unselected span.a-size-small:contains(" Dispatched from: ") + span.a-size-small::text').get(
            '')
        shipped = shipped or response.css('.offer-display-feature-text .offer-display-feature-text-message::text').get('')

        return shipped

    def get_seller_name(self, response):
        'PC Renewed'
        sold = response.css(
            '.a-section.show-on-unselected .a-row:nth-child(2) .truncate .a-size-small:nth-child(2)::text').get('')
        sold = sold or response.css(
            '.a-section.show-on-unselected span.a-size-small:contains(" Sold by:") + span.a-size-small::text').get(
            '')
        sold = sold or response.css('.a-profile-descriptor::text').get('')
        sold = sold or response.css('[tabular-attribute-name="Sold by"] .tabular-buybox-text-message a::text').get(
            '')
        sold = sold or response.css('[tabular-attribute-name="Sold by"] .tabular-buybox-text-message span::text').get('')
        sold = sold or response.css('#sellerProfileTriggerId::text').get('').strip()

        return sold

    def get_shipping_cost(self, response):
        # cost = response.css(
        #     'span[data-csa-c-delivery-type="delivery"]:not(:contains("FREE"))::attr(data-csa-c-delivery-price)').get(
        #     '').replace('£', '').replace('fastest', '').replace('FREE', '')
        cost = response.css(
            'span[data-csa-c-delivery-type="delivery"]::attr(data-csa-c-delivery-price)').get(
            '').replace('£', '').replace('FREE', '').replace('fastest', '')

        return cost

    def get_brand_name(self, response):
        brand = response.css('.po-brand .po-break-word::text').get('')
        brand = brand or response.css('#brand::text').get('')
        brand = brand or response.css('a#bylineInfo::text').get('').strip().lstrip('Brand:')

        return brand
