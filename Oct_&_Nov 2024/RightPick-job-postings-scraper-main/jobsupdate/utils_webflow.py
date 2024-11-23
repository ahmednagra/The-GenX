from dotenv import load_dotenv, find_dotenv
import os
import requests
import json
from pprint import pprint
from time import sleep
import logging

load_dotenv(find_dotenv())
api_key = os.getenv("WEBFLOW_API_KEY")
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Bearer {api_key}"
}
COLLECTION_ID = "64a9d6b46b72ccc0d3030371"

SLEEP_TIME = 1

def make_request(method, url, json=None, headers=headers):
    while True:
        sleep(SLEEP_TIME)
        response = method(url, headers=headers, json=json)
        if not rate_limit_encountered(response):
            return response

def rate_limit_encountered(response):
    if response.status_code == 429:
        logging.error(f"🚨 Rate limit hit! Sleeping for {SLEEP_TIME} seconds...")
        sleep(SLEEP_TIME)
        return True
    return False

subcollection_id_cache = {}

def get_subcollection_id(main_collection_id, subcollection_name):
    if (main_collection_id, subcollection_name) in subcollection_id_cache:
        return subcollection_id_cache[(main_collection_id, subcollection_name)]
    
    url = f"https://api.webflow.com/collections/{main_collection_id}"
    
    response = make_request(requests.get, url)
    collection = json.loads(response.text)
    
    subcollection_id = next((field['validations']['collectionId'] for field in collection['fields'] if field['name'] == subcollection_name), None)
    
    subcollection_id_cache[(main_collection_id, subcollection_name)] = subcollection_id
    
    return subcollection_id


LOCATIONS_COLLECTION_ID = get_subcollection_id(COLLECTION_ID, "Job - Location")
COUNTRIES_COLLECTION_ID = get_subcollection_id(LOCATIONS_COLLECTION_ID, "Country")
REGIONS_COLLECTION_ID = get_subcollection_id(LOCATIONS_COLLECTION_ID, "Region")



def get_field_slugs(collection_id):
    url = f"https://api.webflow.com/collections/{collection_id}"

    response =  make_request(requests.get, url)
    data = response.json()

    field_slugs = {field["name"]: field["slug"] for field in data["fields"]}

    return field_slugs


def get_referenced_items(collection_id):
    url = f"https://api.webflow.com/collections/{collection_id}"

    response = make_request(requests.get, url)
    collection = json.loads(response.text)
    referenced_items = {}

    for field in collection['fields']:
        if field['type'] == "ItemRef":
            referenced_collection_id = field['validations']['collectionId']
            items_url = f"https://api.webflow.com/collections/{referenced_collection_id}/items"

            
            items_response = make_request(requests.get, items_url)
            # pprint(items_response.text)
            items_response_json = json.loads(items_response.text)
            if 'items' in items_response_json:
                items = items_response_json['items']
            else:
                logging.error(f"🚨 Error: 'items' key not found in response. Response text: {items_response.text}")
                items = []

            referenced_items[field['name']] = items #[{"name": item['name'], "slug": item['slug'], "_id": item['_id']} for item in items]

    return referenced_items

# countries_regions = get_referenced_items(LOCATIONS_COLLECTION_ID)
# pprint(countries_regions)

def add_missing_reference_entry(subcollection_id, entry_names, is_location=False, is_country=False):   
    ids = []
    extra_fields = dict()
    countries_regions = get_referenced_items(LOCATIONS_COLLECTION_ID)

    if is_location or is_country:
        region_name = entry_names[-1]
        list_matching_regions = [item for item in countries_regions["Region"] if item["name"].lower() == region_name.lower()]
        
        if len(list_matching_regions) > 0:
            ids.append(list_matching_regions[0]["_id"])
        else:
            logging.info(f"❗️❗️❗️ Region {region_name} not found in {countries_regions['Region']}")
            ids.append(add_missing_reference_entry(REGIONS_COLLECTION_ID, [region_name])[-1][1])
        
        extra_fields["region"] = ids[-1]

    if is_location:
        country_name = entry_names[-2]
        list_matching_countries = [item for item in countries_regions["Country"] if item["name"].lower() == country_name.lower()]

        if len(list_matching_countries) > 0:
            ids.append(list_matching_countries[0]["_id"])
        else:
            logging.info(f"❗️❗️❗️ Country {country_name} not found in {countries_regions['Country']}")
            ids.append(add_missing_reference_entry(COUNTRIES_COLLECTION_ID, [country_name, region_name], is_country=True)[-1][1])
        
        extra_fields["country"] = ids[-1]
    
    payload = {
        "fields": {
            "name": entry_names[0],
            "_archived": False,
            "_draft": False,
            **extra_fields
        }
    }
    response = make_request(requests.post, f"https://api.webflow.com/collections/{subcollection_id}/items?live=true", json=payload)
    location_data = response.json()
    
    if '_id' in location_data:
        ids.append(location_data["_id"])
    else:
        raise ValueError(f"🚨 Error: '_id' field not found in API response: {location_data}")
    
    # # Publish the item
    # response = requests.put(f"https://api.webflow.com/collections/{subcollection_id}/items/publish", headers=headers, json={ "itemIds": [location_data["_id"]] })
    # # print(response.text)

    return list(zip(entry_names[::-1], ids))