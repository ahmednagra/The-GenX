import asyncio
from pyppeteer import launch
import fire
from tqdm import tqdm
import json
import random
from dataextraction.infer_jobs import get_job_info
import re
from dataextraction.geo import get_city_info

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:74.0) Gecko/20100101 Firefox/74.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15'
]

# Function to extract cities from a string
def extract_cities(city_string):
    match = re.search(r'\(.*?\)', city_string)
    if match:
        # Remove parentheses and split by commas
        cities = match.group(0)[1:-1].split(', ')
        return cities
    else:
        # If no match is found, return the original string in a list
        return [city_string]


async def mckinsey_scraper(max_jobs=None):
    browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
    page = await browser.newPage()

    user_agent = random.choice(user_agents)
    await page.setUserAgent(user_agent)

    jobs = {}
    page_number = 1

    while True:
        try:
            print(f"Loading page {page_number}...")
            await page.goto(f'https://www.mckinsey.com/careers/search-jobs?page={page_number}')

            # Wait for job listings to load
            await page.waitForSelector('ul.job-list')
            job_listings = await page.querySelectorAll('ul.job-list li.job-listing')
            job_count = len(job_listings)
            print(f'Found {job_count} job listings')

            if job_count < page_number * 20 or (max_jobs is not None and job_count >= max_jobs):
                break

            page_number += 1

        except Exception as e:
            print(f'Error loading page {page_number}: {e}')
            return
    
    print(f'All job listings loaded!\n Found {len(job_listings)} job listings')

    for job_idx, job in tqdm(enumerate(job_listings)):
        title = await page.evaluate('(element) => element.querySelector("h2.headline a").innerText', job)
        title = title.replace('Job title\n', '')

        location = await page.evaluate('''
            (element) => {
                const singleCity = element.querySelector('div.city-list-container div.city')
                if (singleCity) {
                    return [singleCity.innerText]
                } else {
                    const showMore = element.querySelector('div.city-list-container li.show-all')
                    if (showMore) {
                        showMore.click()
                        return new Promise((resolve) => {
                            setTimeout(() => {
                                const cities = element.querySelectorAll('div.city-list-container ul.list.list-pipe.animate li.city')
                                resolve(Array.from(cities).map(c => c.innerText))
                            }, 500);
                        });
                    } else {
                        const cities = element.querySelectorAll('div.city-list-container ul.list.list-pipe li.city')
                        return Array.from(cities).map(c => c.innerText)
                    }
                } 
            }
        ''', job)

        # Process the list to handle strings containing multiple cities
        final_location_list = []
        for city_string in location:
            final_location_list.extend(extract_cities(city_string))

        # Get the country and region for each city
        final_location_list = [(city,) + get_city_info(city) for city in final_location_list]

        # print(final_location_list)
        
        description = await page.evaluate('(element) => element.querySelector("p.description").innerText', job)
        description = description.replace('Job description\n', '')

        interest = await page.evaluate('(element) => element.querySelector("p.interests").innerText', job)
        interest = interest.replace('Job interest\n', '')

        url = await page.evaluate('(element) => element.querySelector("h2.headline a").href', job)

        jobs[url] = {
            'title': title,
            'url': url,
            'location': final_location_list,
            'description': description,
            'interest': interest
        }

        job_posting_text = f'Job title: {title}\n\nJob interest: {interest}\n\nDescription:\n {description}'

        job_info = get_job_info(job_posting_text)
        jobs[url].update(job_info)

        if max_jobs is not None and job_idx+1 >= max_jobs:
            break

    await browser.close()

    with open('mckinsey_jobs.json', 'w') as f:
        json.dump(jobs, f)

def run_scraper(max_jobs=None):
    asyncio.run(mckinsey_scraper(max_jobs))

if __name__ == '__main__':
    fire.Fire(run_scraper)
