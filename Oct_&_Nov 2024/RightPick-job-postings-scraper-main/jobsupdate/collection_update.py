import csv
from dotenv import load_dotenv, find_dotenv
import os
from tqdm import tqdm
import requests
import json
from collections import defaultdict
from pprint import pprint
import logging
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jobscraper.spiders import SPIDERS, COMMON_FIELDS
load_dotenv(find_dotenv())
TESTING = os.getenv('TESTING') == 'True'

# import firebase_admin
# from firebase_admin import credentials
# from firebase_admin import firestore

# # Initialize Firebase Admin SDK with the service account from the environment variable.
# load_dotenv(find_dotenv())
# cred = credentials.Certificate(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
# firebase_admin.initialize_app(cred)
# db = firestore.client()


def get_value(field_name, job):
    return job.get(field_name) if job.get(field_name) else None


def get_location_id(city, country):
    city = city.strip().replace(' ', '').title()
    country = country.strip().replace(' ', '').title()
    sanitized_city = ''.join(e for e in city if e.isalnum())
    sanitized_country = ''.join(e for e in country if e.isalnum())
    return f"{sanitized_city}_{sanitized_country}"


def sanitize_location(location):
    # Sanitize the location city: "San Francisco, CA", should be "San Francisco"
    # Assuming location is a list [city, country, region]
    city = location[0]
    if city and ',' in city:
        city = city.split(',')[0].strip()
    return [city, location[1], location[2]]


def preprocess_nested_arrays(job_value, field_name):
    # Preprocess nested arrays: Firestore does not support nested arrays. 
    # If any of the values in the job dictionary are arrays, convert them to maps.

    if isinstance(job_value, list):
        if field_name == 'location_list_flattened':
            return job_value
        # Check if it's a list of lists and the field name is 'location'
        if all(isinstance(item, (tuple, list)) for item in job_value):
            if field_name == 'location':
                # Convert the list of lists/tuples into a map with index as key and city, country, region as sub-keys
                # print("🌍 Found nested array for field 'location'. Converting to map. 🌍")
                return {get_location_id(loc[0], loc[1]):
                            {'city': loc[0], 'country': loc[1], 'region': loc[2]}
                        for loc in job_value}
            else:
                # Flatten the list of lists/tuples into a map with integer indices as keys
                job_value_flattened = [item for sublist in job_value for item in sublist]
                return {str(index): value for index, value in enumerate(job_value_flattened)}
        elif isinstance(job_value, (tuple, list)):
            # Convert a simple list or tuple to a map with index as key
            return {str(index): value for index, value in enumerate(job_value)}
    return job_value


def update_locations_collection(db, location):
    # Sanitize the location data
    city, country, region = sanitize_location(location)

    # Create a unique identifier for the location
    location_id = get_location_id(city, country)

    # Log the location_id to debug
    print(f"🌍 Attempting to update or create in Firestore location with ID: {location_id}")

    # Check if the location already exists in the 'locations' collection
    locations_collection_name = 'locations_test' if TESTING else 'locations'
    location_ref = db.collection(locations_collection_name).document(location_id)
    location_doc = location_ref.get()

    # If the location does not exist, add it to the 'locations' collection
    if not location_doc.exists:
        location_data = {
            'city': city,
            'country': country,
            'region': region
        }
        location_ref.set(location_data)


def upload_job(db, job):
    # Use a default dictionary to handle missing fields in job data.
    job = defaultdict(lambda: None, job)

    field_names_new = COMMON_FIELDS + SPIDERS[job['company']]['custom_fields']

    # Create a dictionary for Firestore document fields with preprocessing for nested arrays.
    fields = {
        field_name: preprocess_nested_arrays(job.get(field_name), field_name)
        for field_name in field_names_new
        if job.get(field_name) is not None
    }

    # Update the 'locations' collection if the job has a location.
    if job['location']:
        for location in job['location']:
            update_locations_collection(db, location)

    # Add a new document in collection 'jobs' with ID 'id_unique', and the job data.
    collection_name = 'jobs_test' if TESTING else 'jobs'
    db.collection(collection_name).document(job['id_unique']).set(fields)


def remove_obsolete_jobs(company, spider, db, scraped_jobs_dict):
    """
    Remove jobs from the 'jobs' collection in Firestore that are not present in the scraped_jobs_dict.

    Parameters:
    db (firestore.client.Client): A Firestore client object.
    scraped_jobs_dict (dict): A dictionary of scraped jobs.
    """
    collection_name = 'jobs_test' if TESTING else 'jobs'
    jobs_collection = db.collection(collection_name)
    # Fetch all job documents for the current company
    docs = jobs_collection.where('company', '==', company).stream()
    for doc in docs:
        doc_dict = doc.to_dict()
        if doc_dict['id_unique'] not in scraped_jobs_dict:
            print(
                f"🗑️ Removing outdated job: {doc_dict['title']} from {doc_dict['company_title']} (URL: {doc_dict['url']}) with id_unique: {doc_dict['id_unique']})")
            spider.logger.info(
                f"🗑️ Removing outdated job: {doc_dict['title']} from {doc_dict['company_title']} (URL: {doc_dict['url']}) with id_unique: {doc_dict['id_unique']})")
            jobs_collection.document(doc.id).delete()

# Example of a location list:
# location = [('Atlanta', 'United States', 'North America'), ('Austin', 'United States', 'North America'),
#             ('Boston', 'United States', 'North America'), ('Brooklyn', 'United States', 'North America'),
#             ('Chicago', 'United States', 'North America')]
