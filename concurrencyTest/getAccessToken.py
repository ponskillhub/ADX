import requests
from config import config

token_endpoint = f"https://login.microsoftonline.com/{config['tenant_id']}/oauth2/token"
client_id = config['client_id']
client_secret = config['client_secret']
resource = "https://api.kusto.windows.net"

# Create the payload for the request
payload = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "resource": resource
}

# Send the POST request to the token endpoint
response = requests.post(token_endpoint, data=payload)

# Check the response status
if response.status_code == 200:
    # Extract the access token from the response
    access_token = response.json()["access_token"]
    print("Access token:", access_token)
else:
    print("Error:", response.text)


