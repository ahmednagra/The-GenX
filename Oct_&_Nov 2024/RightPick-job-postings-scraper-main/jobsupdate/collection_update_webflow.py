import csv
from dotenv import load_dotenv, find_dotenv
import os
from tqdm import tqdm
import requests
import json
from slugify import slugify
from collections import defaultdict
from webflowupdate.utils import get_field_slugs, get_referenced_items, add_missing_reference_entry, make_request, LOCATIONS_COLLECTION_ID
from pprint import pprint
import logging

load_dotenv(find_dotenv())
api_key = os.getenv("WEBFLOW_API_KEY")
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Bearer {api_key}"
}

COLLECTION_ID = "64a9d6b46b72ccc0d3030371"

field_slugs = get_field_slugs(COLLECTION_ID)

companies = {
    "mckinsey": "McKinsey & Company",
    "bain": "Bain & Company",
    "bcg": "Boston Consulting Group",
    "goldmansachs": "Goldman Sachs",
    "meta": "Meta"
}

def get_value(field, job_value):

    referenced_items = get_referenced_items(COLLECTION_ID)

    if field == "Job - Location":
        locations_list = []

        for city, ctry, reg in job_value:
            list_items = [item for item in referenced_items[field] if item["name"].lower() == city.lower()]
            
            if len(list_items) > 0:
                city_id = list_items[0]["_id"]
                ctry_id = list_items[0]["country"]
                reg_id = list_items[0]["region"]
                locations_list.append([(reg, reg_id), (ctry, ctry_id), (city, city_id)])
            else:
                locations_list.append(add_missing_reference_entry(LOCATIONS_COLLECTION_ID, [city, ctry, reg], is_location = True))
        return locations_list
    
    elif field in referenced_items:
        logging.info(f"🔎 Looking for {job_value} in {field}") #: {referenced_items[field]}")
        if job_value == None:
            logging.info(f"🚫 {job_value} not found in {field}")
            return None
        
        list_items = [item for item in referenced_items[field] if item["name"].lower() == job_value.lower()]
        
        if len(list_items) > 0:
            return list_items[0]["_id"]
        else:
            raise ValueError(f"⛔️ Could not find {job_value} in {field}")
    else:
        return job_value

    
field_names = ["Job - Title", "Job - Slug", "Job - Description", "Job - Requirements", "Job - Responsibilities", "Job - Seniority", "Job - Industry", "Job - Salary", "Job - Company", "Job - Application Link", "Job - Custom Id"]


def upload_job(job):
    job = defaultdict(lambda: None, job)

    job_values = [job["title"], slugify(job["title"]), job["description"], job["requirements"], job["responsibilities"], job["seniority"], job["industry"], job["salary"], companies[job["company"]], job['url'], job['id_unique']]

    pre_fields = {field_slugs[field_name]: get_value(field_name, job_value) for field_name, job_value in zip(field_names, job_values) if job_value is not None}

    locations = get_value("Job - Location", job["location"])

    for location in locations:
        (reg, reg_id), (ctry, ctry_id), (city, city_id) = location
        logging.info(f"🆕 Adding {job['title']} in {city}, {ctry} ({reg})")
        loc_dict = {field_slugs["Job - Location"]: city_id, field_slugs["Job - Country"]: ctry_id} #, field_slugs["Job - Region"]: reg_id}

        payload = {
            "fields": {**pre_fields, **loc_dict, "_archived": False, "_draft": False}
        }
        # print(payload)
        response = make_request(requests.post, f"https://api.webflow.com/collections/{COLLECTION_ID}/items?live=true", json=payload, headers=headers)
        # pprint(response.json())

        # # Publish changes 
        # response = requests.put(f"https://api.webflow.com/collections/{COLLECTION_ID}/items/publish", headers=headers)
        # print(response.text)

# with open('data/global_jobs.json') as f:
#     data = json.load(f)
#     # TODO: handle badly formatted json files because of scrapy's json append feature
#     for job in data:
#         upload_job(job)


# TODO: multi-references support
# TODO: add locations that don't exist in webflow