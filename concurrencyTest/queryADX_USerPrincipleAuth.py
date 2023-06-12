from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
import pandas as pd
import threading
from config import config

NUM_OF_THREADS = config['number_of_threads']

AAD_TENANT_ID = config['tenant_id']
KUSTO_CLUSTER = config['cluster']
KUSTO_DATABASE = config['database']

# KCSB = KustoConnectionStringBuilder.with_aad_device_authentication(
#     KUSTO_CLUSTER)
# KCSB.authority_id = AAD_TENANT_ID

username = config['username']
password = config['password']
KCSB = KustoConnectionStringBuilder.with_aad_user_password_authentication(
    KUSTO_CLUSTER, username, password, AAD_TENANT_ID)

KUSTO_CLIENT = KustoClient(KCSB)

QUERIES = ["StormEvents | sort by StartTime desc | take 1",
           "StormEvents | sort by EndTime desc | take 1",
           "StormEvents | sort by EpisodeId desc | take 1",
           "StormEvents | sort by EventId desc | take 1",
           "StormEvents | sort by State desc | take 1",
           "StormEvents | sort by EventType desc | take 1",
           "StormEvents | sort by InjuriesDirect desc | take 1",
           "StormEvents | sort by DeathsDirect desc | take 1",
           "StormEvents | sort by DeathsIndirect desc | take 1",
           "StormEvents | sort by DamageProperty desc | take 1",
           "StormEvents | sort by DamageCrops desc | take 1",
           "StormEvents | sort by Source desc | take 1",
           "StormEvents | sort by BeginLocation desc | take 1",
           "StormEvents | sort by EndLocation desc | take 1",
           "StormEvents | sort by BeginLat desc | take 1"]


def execute_query(KUSTO_QUERY):
    try:
        # Execute the query
        RESPONSE = KUSTO_CLIENT.execute(KUSTO_DATABASE, KUSTO_QUERY)

    except KustoServiceError as error:
        print(f"Error executing query: {error}")


execute_query("StormEvents | sort by StartTime desc | take 1")


def execute_batch_queries(QUERIES):
    for KUSTO_QUERY in QUERIES:
        execute_query(KUSTO_QUERY)


threads = []

for i in range(0, NUM_OF_THREADS):
    thread = threading.Thread(target=execute_batch_queries, args=(QUERIES,))
    thread.start()
    threads.append(thread)

# wait for all threads to complete
print("*" * 50)

print("Number of threads: " + str(NUM_OF_THREADS))
print("Number of queries: " + str(len(QUERIES)))

total_num_of_cuncurrent_queries = NUM_OF_THREADS * len(QUERIES)
print("total_num_of_cuncurrent_queries: " +
      str(total_num_of_cuncurrent_queries))

starttime = pd.Timestamp.now()
print("starttime time : " + str(starttime))
for thread in threads:
    thread.join()
endtime = pd.Timestamp.now()
print("endtime time : " + str(endtime))

totaltime = endtime - starttime
print("total time : " + str(totaltime))

print("*" * 50)
