# from jobscraper.settings import SEEN_JOBS_FILE
import datetime
import json
import os
import time

import firebase_admin
from dotenv import load_dotenv, find_dotenv
from firebase_admin import credentials
from firebase_admin import firestore

CACHE_FILE = 'seen_jobs_cache.json'
CACHE_EXPIRY = 23 * 60 * 60  # 23 hours in seconds

load_dotenv(find_dotenv())
TESTING = os.getenv('TESTING') == 'True'

# Check if the default app has already been initialized
if not firebase_admin._apps:
    # Initialize Firebase Admin SDK with the service account from the environment variable.
    cred = credentials.Certificate(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
    firebase_admin.initialize_app(cred)

db = firestore.client()

def old_get_seen_jobs(company_name):
    # Reference to the company's seen jobs collection.
    company_jobs_ref = db.collection('companies').document(company_name).collection('seen_jobs')
    
    # Attempt to get the documents within the collection and return only job ids.
    try:
        seen_jobs_docs = company_jobs_ref.stream()
        seen_jobs_ids = {doc.id for doc in seen_jobs_docs}
    except Exception as e:
        print(f"🚨 'Seen jobs' error occurred: {e}")
        seen_jobs_ids = set()
    
    return seen_jobs_ids

def old_write_seen_jobs(company_name, current_jobs_list):
    # Reference to the company's seen jobs collection.
    company_jobs_ref = db.collection('companies').document(company_name).collection('seen_jobs')
    
    # Write each job to Firestore.
    for human_readable_id, job_id in current_jobs_list:
        job_ref = company_jobs_ref.document(job_id)
        job_ref.set({'human_readable_id': human_readable_id})

def ensure_cache_file():
    if not os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'w') as f:
            json.dump({"timestamps": {}, "data": {}}, f)
        os.chmod(CACHE_FILE, 0o777)

def load_cache():
    ensure_cache_file()
    try:
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        raise Exception(f"🚨🚨🚨🚨 Failed to load cache: {e}")

def save_cache(cache):
    ensure_cache_file()
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)


def get_seen_jobs(company_name):
    try:
        cache = load_cache()
    except Exception as e:
        raise Exception(f"Failed to load cache: {e}")

    company_timestamp = cache['timestamps'].get(company_name, 0)
    current_time = time.time()

    readable_company_timestamp = datetime.datetime.utcfromtimestamp(company_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    readable_current_time = datetime.datetime.utcfromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')

    force_cache_computation = TESTING and os.getenv('FORCE_ALREADY_SEEN_CACHE_COMPUTATION') == 'True'
    force_no_seen_jobs = TESTING and os.getenv('FORCE_NO_SEEN_JOBS') == 'True'

    if force_cache_computation or force_no_seen_jobs or (current_time - company_timestamp > CACHE_EXPIRY):
        # Cache is expired or forced, fetch new data
        if force_no_seen_jobs:
            seen_jobs_ids = set()
        else:
            try:
                collection_name = 'jobs_test' if TESTING else 'jobs'
                jobs_ref = db.collection(collection_name)
                seen_jobs_docs = jobs_ref.where('company', '==', company_name).stream()
                seen_jobs_ids = {doc.get('id_unique') for doc in seen_jobs_docs}
            except Exception as e:
                raise Exception(f"🚨🚨🚨🚨 Failed to fetch seen jobs: {e}")

        # Update cache
        cache["data"][company_name] = list(seen_jobs_ids)
        cache["timestamps"][company_name] = current_time
        save_cache(cache)
    else:
        # Use cached data
        seen_jobs_ids = set(cache["data"].get(company_name, []))

    return seen_jobs_ids

def create_seen_jobs_json():
    # Get a reference to the companies collection.
    companies_ref = db.collection('companies')
    
    all_seen_jobs = {}
    
    # Stream through each company document.
    for company_doc in companies_ref.stream():
        company_name = company_doc.id
        company_jobs_ref = company_doc.reference.collection('seen_jobs')
        
        # Fetch seen jobs for the company.
        seen_jobs_docs = company_jobs_ref.stream()
        seen_jobs_list = [(doc.get('human_readable_id'), doc.id) for doc in seen_jobs_docs]
        
        # Add to the all_seen_jobs dictionary.
        all_seen_jobs[company_name] = seen_jobs_list
    
    # Write the dictionary to a JSON file.
    with open('seen_jobs.json', 'w') as json_file:
        json.dump(all_seen_jobs, json_file, indent=4)