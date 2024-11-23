from google.oauth2 import service_account
import google.auth.transport.requests

# Path to your service account JSON file
SERVICE_ACCOUNT_FILE = '../rightpick-firebase.json'

# Create credentials
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=['https://www.googleapis.com/auth/firebase']
)

# Use the credentials to get an access token
auth_req = google.auth.transport.requests.Request()
credentials.refresh(auth_req)

print(credentials.token)