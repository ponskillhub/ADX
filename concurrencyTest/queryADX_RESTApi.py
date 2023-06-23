import requests
from config import config
import json

token_endpoint = f"https://login.microsoftonline.com/{config['tenant_id']}/oauth2/token"
client_id = config['client_id']
client_secret = config['client_secret']
resource = "https://api.kusto.windows.net"
database = config['database']
cluster = config['cluster']


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

url = f"{cluster}/v2/rest/query"

payload = {
    "db": f"{database}",
    "csl": "StormEvents | sort by StartTime desc | take 1",
    "properties": {
        "Options": {
            "queryconsistency": "strongconsistency"
        }
    }
}

headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json; charset=utf-8",
    "Host": "help.kusto.windows.net",
}

response = requests.post(url, data=json.dumps(payload), headers=headers)

if response.status_code == 200:
    data = response.json()
    # Process the response data
    print("Response:", data['Rows'])
    print("Status code:", response.status_code)
else:
    print("Error:", response.text)
    print("Status code:", response.status_code)
