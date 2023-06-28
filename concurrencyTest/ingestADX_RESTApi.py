To write records into Azure Data Explorer using the REST API in Python, you can use the `requests` library. Here's an example program that demonstrates how to do it:

```python
import requests
import json

# Set the required variables
cluster_name = "<your_cluster_name>"
database_name = "<your_database_name>"
table_name = "<your_table_name>"
api_endpoint = f"https://{cluster_name}.kusto.windows.net/v1/rest/mgmt"

# Set the authentication details
client_id = "<your_client_id>"
client_secret = "<your_client_secret>"
tenant_id = "<your_tenant_id>"

# Get the access token
auth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "resource": "https://management.core.windows.net/",
}
response = requests.post(auth_url, data=data)
access_token = response.json()["access_token"]

# Set the ingestion endpoint
ingest_endpoint = f"https://{cluster_name}.kusto.windows.net/v1/rest/mgmt"

# Set the data to be ingested
data_to_ingest = [
    {"ColumnName1": "Value1", "ColumnName2": "Value2"},
    {"ColumnName1": "Value3", "ColumnName2": "Value4"},
    # Add more records as needed
]

# Convert the data to JSON
json_data = json.dumps(data_to_ingest)

# Set the ingestion properties
ingestion_properties = {
    "format": "json",
    "ingestionMappingReference": f"{database_name}/{table_name}/ingestionMapping",
    # Add more properties if required
}

# Construct the ingestion request
ingestion_request = {
    "jsonMappingReference": "ingestionMapping",
    "ingestionProperties": ingestion_properties,
    "data": json_data,
}

# Send the ingestion request
ingest_url = f"{ingest_endpoint}/{database_name}/{table_name}/ingest"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}
response = requests.post(ingest_url, headers=headers, json=ingestion_request)

# Check the response
if response.status_code == 202:
    print("Data ingestion request submitted successfully.")
else:
    print("Data ingestion request failed. Status code:", response.status_code)
    print("Response:", response.text)
