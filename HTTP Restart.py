import os
import logging
import azure.functions as func
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Retrieve the filename from the query parameters
    filename = req.params.get('filename')
    if not filename:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            filename = req_body.get('filename')

    if filename:
        # Check if the file extension is CSV
        if filename.lower().endswith('.csv'):
            try:
                # Initialize ManagedIdentityCredential
                credential = ManagedIdentityCredential()

                # Initialize BlobServiceClient with managed identity and private endpoint
                blob_service_client = BlobServiceClient(
                    account_url="https://<private_endpoint_name>.blob.core.windows.net",
                    credential=credential
                )

                # Get the blob client for the existing file
                source_blob_client = blob_service_client.get_blob_client(container="<container_name>", blob=filename)

                # Get the blob client for the destination file (same name, overwriting)
                destination_blob_client = blob_service_client.get_blob_client(container="<container_name>", blob=filename)

                # Start the copy operation
                destination_blob_client.start_copy_from_url(source_blob_client.url)

                logging.info(f"File '{filename}' moved successfully.")
                return func.HttpResponse(f"File '{filename}' moved successfully.", status_code=200)
            except Exception as e:
                logging.error(f"An error occurred: {e}")
                return func.HttpResponse("Internal server error", status_code=500)
        else:
            return func.HttpResponse(
                "The file extension is not CSV.",
                status_code=400
            )
    else:
        return func.HttpResponse(
            "Please pass a filename on the query string or in the request body",
            status_code=400
        )