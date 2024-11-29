import firebase_admin
from firebase_admin import credentials, auth, firestore, storage
import argparse
from dotenv import load_dotenv, find_dotenv
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(find_dotenv())
TESTING = os.getenv('TESTING') == 'True'

# This script removes users from Firebase: it deletes users from Firebase Authentication,
# removes their documents from Firestore, and deletes their profile images from Firebase Storage.

# To run this script, provide the path to the Firebase Admin SDK JSON file
# and the list of user auth IDs you want to remove as command-line arguments.

# Example usage:
# python remove_users.py /path/to/firebase-adminsdk.json user_auth_id_1 user_auth_id_2

load_dotenv(find_dotenv())

# Initialize the argument parser
parser = argparse.ArgumentParser(description='🗑️ Remove users from Firebase based on their auth IDs.')

# Add arguments to the parser
parser.add_argument('user_ids', nargs='+', help='🆔 List of user auth IDs to remove')

# Parse the arguments
args = parser.parse_args()


# Check if the default app has already been initialized
if not firebase_admin._apps:
    # Initialize Firebase Admin SDK with the service account from the environment variable.
    cred = credentials.Certificate(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
    firebase_admin.initialize_app(cred)

# Firebase Firestore and Storage clients
db = firestore.client()
bucket = storage.bucket()

# Iterate over the list of user IDs provided in the command line
for user_id in args.user_ids:
    try:
        # Delete user from Firebase Authentication
        auth.delete_user(user_id)
        print(f'🗑️ Auth user {user_id} deleted.')

        # Delete user document from Firestore
        collection_name = 'users_test' if TESTING else 'users'
        db.collection(collection_name).document(user_id).delete()
        print(f'🗑️ Firestore user document {user_id} deleted.')

        # Delete user's profile image from Firebase Storage
        profile_image_path = f'profile_images/{user_id}'
        blob = bucket.blob(profile_image_path)
        blob.delete()
        print(f'🗑️ Storage profile image {user_id} deleted.')

    except Exception as e:
        print(f'❌ Error removing user {user_id}: {e}')

print('✅ User removal process completed.')
