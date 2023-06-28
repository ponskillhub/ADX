import requests
from datetime import datetime


class IngestionResourcesSnapshot:
    def __init__(self):
        self.IngestionQueues = []
        self.TempStorageContainers = []
        self.FailureNotificationsQueue = ''
        self.SuccessNotificationsQueue = ''


def IngestSingleFile(file, db, table, ingestionMappingRef):
    # Your Azure Data Explorer ingestion service URI, typically ingest-<your cluster name>.kusto.windows.net
    dmServiceBaseUri = "https://ingest-{serviceNameAndRegion}.kusto.windows.net"
    # 1. Authenticate the interactive user (or application) to access Kusto ingestion service
    bearerToken = AuthenticateInteractiveUser(dmServiceBaseUri)
    # 2a. Retrieve ingestion resources
    ingestionResources = RetrieveIngestionResources(
        dmServiceBaseUri, bearerToken)
    # 2b. Retrieve Kusto identity token
    identityToken = RetrieveKustoIdentityToken(dmServiceBaseUri, bearerToken)
    # 3. Upload file to one of the blob containers we got from Azure Data Explorer.
    # This example uses the first one, but when working with multiple blobs,
    # one should round-robin the containers in order to prevent throttling
    blobName = f"TestData{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S.%f')}"
    blobUriWithSas, blobSizeBytes = UploadFileToBlobContainer(
        file, ingestionResources.TempStorageContainers[0], blobName)
    # 4. Compose ingestion command
    ingestionMessage = PrepareIngestionMessage(
        db, table, blobUriWithSas, blobSizeBytes, ingestionMappingRef, identityToken)
    # 5. Post ingestion command to one of the previously obtained ingestion queues.
    # This example uses the first one, but when working with multiple blobs,
    # one should round-robin the queues in order to prevent throttling
    PostMessageToQueue(ingestionResources.IngestionQueues[0], ingestionMessage)

    time.sleep(20)

    # 6a. Read success notifications
    successes = PopTopMessagesFromQueue(
        ingestionResources.SuccessNotificationsQueue, 32)
    for sm in successes:
        print(f"Ingestion completed: {sm}")

    # 6b. Read failure notifications
    errors = PopTopMessagesFromQueue(
        ingestionResources.FailureNotificationsQueue, 32)
    for em in errors:
        print(f"Ingestion error: {em}")
