# pyton program to query ADX REST API using client authentication

import requests
import json
import time
import pandas as pd
import threading
from config import config

NUM_OF_THREADS = config['number_of_threads']

AAD_TENANT_ID = config['tenant_id']
KUSTO_CLUSTER = config['cluster']
KUSTO_DATABASE = config['database']


client_id = config['client_id']
client_secret = config['client_secret']

QUERIES = ["StormEvents | sort by StartTime desc | take 1",
           "StormEvents | sort by EndTime desc | take 1"]


def get_access_token():
    url = f"https://login.microsoftonline.com/{AAD_TENANT_ID}/oauth2/token"
    payload = f"grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&resource=https://management.core.windows.net/"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()['access_token']


access_token = get_access_token()

print("access_token: ", access_token)


def execute_query(KUSTO_QUERY):
    try:
        # Execute the query
        url = f"https://{KUSTO_CLUSTER}.kusto.windows.net/v2/rest/query"
        payload = f"{KUSTO_QUERY}"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'Host': f'{KUSTO_CLUSTER}.kusto.windows.net'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        print(response.text.encode('utf8'))
    except Exception as error:
        print(f"Error executing query: {error}")


def execute_batch_queries(QUERIES):
    for KUSTO_QUERY in QUERIES:
        execute_query(KUSTO_QUERY)


def execute_batch_queries_threaded(QUERIES):
    threads = []
    for i in range(NUM_OF_THREADS):
        thread = threading.Thread(
            target=execute_batch_queries, args=(QUERIES,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()


execute_batch_queries_threaded(QUERIES)
