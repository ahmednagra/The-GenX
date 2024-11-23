import os
from urllib.parse import urlencode
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
API_KEY = os.getenv('SCRAPEOPS_API_KEY')

def get_proxy_url(url):
    payload = {
        'api_key': API_KEY, 
        'url': url, 
        'render_js': True
    }
    proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
    return proxy_url