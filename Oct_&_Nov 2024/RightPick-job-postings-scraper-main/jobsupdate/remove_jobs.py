import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
import argparse
from dotenv import load_dotenv, find_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from google.api_core.exceptions import DeadlineExceeded
from tqdm import tqdm
import time
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from collection_update import get_location_id
from dataextraction import extract_cities_parens
load_dotenv(find_dotenv())
TESTING = os.getenv('TESTING') == 'True'

# This script is used in the following manner: a new user may execute it by supplying a sequence of job IDs as arguments. 
# For instance:
# python remove_jobs.py job_id_1 job_id_2 job_id_3
# python remove_jobs.py --company "goldmansachs"

# The script will remove the designated jobs and then refresh the locations collection accordingly.

load_dotenv(find_dotenv())

# Check if the default app has already been initialized
if not firebase_admin._apps:
    # Initialize Firebase Admin SDK with the service account from the environment variable.
    cred = credentials.Certificate(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
    firebase_admin.initialize_app(cred)

db = firestore.client()

def remove_jobs(job_ids_to_remove):
    """
    Remove jobs from the 'jobs' collection in Firestore.

    Parameters:
    job_ids_to_remove (list of str): A list of job IDs to be removed.
    """
    for job_id in job_ids_to_remove:
        collection_name = 'jobs_test' if TESTING else 'jobs'
        db.collection(collection_name).document(job_id).delete()
        print(f"🗑️ Deleted job with ID: {job_id}")

def remove_jobs_from_company(company):
    """
    Remove jobs from the 'jobs' collection in Firestore for a specific company.
    """
    collection_name = 'jobs_test' if TESTING else 'jobs'
    jobs = list(db.collection(collection_name).where('company', '==', company).stream())

    for job in tqdm(jobs, desc=f"Removing jobs from {company}"):
        db.collection(collection_name).document(job.id).delete()
        print(f"🗑️ Deleted job with ID: {job.id} from company: {company}")

def update_locations_collection():
    """
    Update the 'locations' collection in Firestore by removing locations that no longer have any associated jobs.
    """
    jobs_collection_name = 'jobs_test' if TESTING else 'jobs'
    locations_collection_name = 'locations_test' if TESTING else 'locations'

    locations = db.collection(locations_collection_name).stream()

    for location in tqdm(locations, desc="Updating locations collection"):
        try:
            jobs = db.collection(jobs_collection_name).where(f'location.{location.id}.city', '>', '').limit(1).get()
            if not jobs:
                db.collection(locations_collection_name).document(location.id).delete()
                print(f"📍 Deleted location with ID: {location.id}")
        except Exception as e:
            print(f"Error fetching jobs for location ID {location.id}: {e}")

# Define retry strategy
@retry(
    retry=retry_if_exception_type(DeadlineExceeded),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5)
)
def firestore_operation_with_retry(operation):
    """
    Attempt a Firestore operation with retries on DeadlineExceeded error.

    Parameters:
    operation (callable): The Firestore operation to attempt.
    """
    return operation()

def restore_locations_collection():
    """
    Restore locations to the 'locations' collection in Firestore by extracting them from the 'jobs' collection.
    """
    jobs_collection_name = 'jobs_test' if TESTING else 'jobs'
    locations_collection_name = 'locations_test' if TESTING else 'locations'

    try:
        all_jobs = firestore_operation_with_retry(lambda: db.collection(jobs_collection_name).stream())
    except Exception as e:
        print(f"Failed to fetch jobs: {e}")
        return
    unique_locations = {}
    for job in tqdm(all_jobs, desc="Processing jobs for location restoration"):
        job_data = job.to_dict()
        updated_location_data = {}
        location_list_flattened_update = []
        changed_city_name = False
        changed_location_id = False
        for location_id, location_data in job_data.get('location', {}).items():
            # Extract and sanitize city name
            original_city = location_data.get('city', '')
            sanitized_city = extract_cities_parens(original_city)[0]
            # Update city in location data if sanitized city is different
            if sanitized_city != original_city:
                location_data['city'] = sanitized_city
                changed_city_name = True
                print(f"🏙️ Updated city name from '{original_city}' to '{sanitized_city}'")

            sanitized_location_id = get_location_id(location_data.get('city', ''), location_data.get('country', ''))
            if sanitized_location_id != location_id:  # Check if the location ID needs to be updated
                updated_location_data[sanitized_location_id] = location_data
                changed_location_id = True
            else:
                updated_location_data[location_id] = location_data

            if sanitized_location_id not in unique_locations:
                unique_locations[sanitized_location_id] = location_data

            # Add the city, country, and region as separate elements to the flattened list
            if 'location_list_flattened' not in job_data:
                if city := location_data.get('city'):
                    location_list_flattened_update.append(city)
                if country := location_data.get('country'):
                    location_list_flattened_update.append(country)
                if region := location_data.get('region'):
                    location_list_flattened_update.append(region)

        # Check if 'location_list_flattened' needs to be updated or added
        if changed_city_name or changed_location_id or 'location_list_flattened' not in job_data:
            try:
                firestore_operation_with_retry(lambda: db.collection(jobs_collection_name).document(job.id).update({
                    'location': updated_location_data,
                    'location_list_flattened': list(set(location_list_flattened_update))
                }))
                print(f"🔄 Updated job with ID: {job.id}")
                print(f"📍 Updated location data: {updated_location_data}")
                print(f"📍 Updated location list flattened: {list(set(location_list_flattened_update))}")
            except Exception as e:
                print(f"Failed to update job {job.id}: {e}")

        # time.sleep(0.2)

    for sanitized_location_id, location_data in tqdm(unique_locations.items(), desc="Restoring/Adding locations"):
        try:
            firestore_operation_with_retry(lambda: db.collection(locations_collection_name).document(sanitized_location_id).set(location_data))
            print(f"✅ Restored/Added location with ID: {sanitized_location_id}")
        except Exception as e:
            print(f"Failed to restore/add location {sanitized_location_id}: {e}")
        # time.sleep(0.2)

@retry(
    retry=retry_if_exception_type(DeadlineExceeded),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5)
)
def transform_company_jobs(company, transform_function):
    """
    Apply a transformation to all job entries of a given company.

    Parameters:
    company (str): The name of the company whose jobs need to be transformed.
    transform_function (callable): A function that takes a job document and returns the transformed job data.

    Example usage: transform_company_jobs('google', fix_google_job_urls)
    """
    collection_name = 'jobs_test' if TESTING else 'jobs'
    jobs = firestore_operation_with_retry(lambda: db.collection(collection_name).where('company', '==', company).stream())

    total_jobs = 0
    updated_jobs = 0

    for job in tqdm(jobs, desc=f"Transforming jobs for {company}"):
        total_jobs += 1
        job_data = job.to_dict()
        transformed_data = transform_function(job_data)
        
        # Compute the difference between the original and transformed data
        updates = {k: v for k, v in transformed_data.items() if v != job_data.get(k)}
        
        if updates:
            try:
                firestore_operation_with_retry(lambda: db.collection(collection_name).document(job.id).update(updates))
                updated_jobs += 1
                print(f"✅ Updated job number {total_jobs}, with ID: {job.id}")
                for key, value in updates.items():
                    print(f"  Updated {key}: {job_data.get(key)} -> {value}")
            except Exception as e:
                print(f"Failed to update job number {total_jobs}, with ID: {job.id}: {e}")
        else:
            print(f"No update needed for job number {total_jobs}, with ID: {job.id}")

    print(f"📊 Processed {total_jobs} jobs for {company}")
    print(f"✅ Updated {updated_jobs} jobs")

def fix_google_job_urls(job_data):
    """
    Transform function to fix Google job URLs.
    Example usage: transform_company_jobs('google', fix_google_job_urls)
    """
    if 'url' in job_data and not job_data['url'].startswith('http'):
        new_job_data = job_data.copy()
        new_job_data['url'] = f"https://www.google.com/about/careers/applications/{job_data['url']}"
        print(f"🔗 Updated URL from '{job_data['url']}' to '{new_job_data['url']}'")
        return new_job_data
    return job_data

@retry(
    retry=retry_if_exception_type(DeadlineExceeded),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5)
)
def reset_all_likes():
    """
    Reset all job likes/dislikes to neutral.
    """
    # Reset all jobs
    jobs_ref = db.collection('jobs')
    jobs = jobs_ref.get()

    for job in jobs:
        job_ref = jobs_ref.document(job.id)
        job_ref.update({
            'numberOfLikes': 0,
            'numberOfDislikes': 0
        })
        print(f"🔄 Reset job with ID: {job.id} to neutral.")

    print(f"❤️‍🩹 Reset {len(jobs)} jobs to neutral.")

    # Delete all entries in the 'likes' collection
    likes_ref = db.collection('likes')
    likes = likes_ref.get()

    for like in likes:
        like.reference.delete()
        print(f"🗑️ Deleted like with ID: {like.id}")

    print(f"🗑️ Deleted {len(likes)} entries from the 'likes' collection.")

    print("✅ Job votes reinitialization complete.")


def main(job_ids, company=None):
    """
    Processes the command line arguments and calls the appropriate functions.

    Parameters:
    job_ids (list of str): A list of job IDs to be removed.
    company (str, optional): The name of the company from which to remove jobs.
    """
    if company:
        remove_jobs_from_company(company)
    else:
        remove_jobs(job_ids)
    update_locations_collection()
    
    # restore_locations_collection()
    # reset_all_likes()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="🗑️ Remove obsolete jobs and update locations in Firestore.")
    parser.add_argument('job_ids', nargs='*', help='🆔 A list of job IDs to remove. Pass multiple IDs separated by spaces.')
    parser.add_argument('--company', type=str, help='🏢 Company name to remove all jobs from.')

    args = parser.parse_args()

    if args.company:
        main([], args.company)
    else:
        main(args.job_ids)
