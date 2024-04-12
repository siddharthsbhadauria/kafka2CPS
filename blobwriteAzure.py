from azure.storage.blob import BlobServiceClient
from azure.identity import ManagedIdentityCredential

def write_stream_to_blob(stream, account_url, container_name, blob_name):
    """
    Write a stream of data to Azure Blob Storage.

    Parameters:
        stream (bytes-like object): The stream of data to be uploaded.
        account_url (str): The URL of the Azure Blob Storage account.
        container_name (str): The name of the container in Azure Blob Storage.
        blob_name (str): The name of the blob to which the data will be written.
    """
    try:
        # Authenticate with User-Assigned Managed Identity
        credential = ManagedIdentityCredential()

        # Create a BlobServiceClient using a private endpoint
        blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=credential
        )

        # Get a container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Upload stream data as a blob
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(stream)
        
        print("Stream data uploaded successfully.")
    except Exception as e:
        # Log the error and raise it to the caller
        error_message = f"Error uploading stream data to Azure Blob Storage: {str(e)}"
        print(error_message)
        raise e