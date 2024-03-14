import sys
from azure.identity import DefaultAzureCredential, CredentialUnavailableError
from azure.storage.blob import BlobServiceClient, ResourceNotFoundError, ClientAuthenticationError
import os

if len(sys.argv) != 6:
    print("Usage: python script.py tenant_id source_file_path target_path pem_file_path client_id")
    sys.exit(1)

# Extract command-line arguments
tenant_id = sys.argv[1]
source_file_path = sys.argv[2]
target_path = sys.argv[3]
pem_file_path = sys.argv[4]
client_id = sys.argv[5]

try:
    # Authenticate using service principal and private key
    credential = DefaultAzureCredential(authority=f"https://login.microsoftonline.com/{tenant_id}", client_id=client_id, tenant_id=tenant_id, certificate_path=pem_file_path)

    # Connect to client's storage account
    client = BlobServiceClient(account_url="https://<client_storage_account_name>.blob.core.windows.net", credential=credential)

    # Upload file
    with open(source_file_path, "rb") as data:
        blob_client = client.get_blob_client(container=target_container, blob=os.path.basename(source_file_path))
        blob_client.upload_blob(data)

    print("File uploaded successfully.")

except CredentialUnavailableError:
    print("Credential Unavailable. Please check your credentials and try again.")
except ResourceNotFoundError:
    print("Resource not found. Please check the target container name and try again.")
except ClientAuthenticationError:
    print("Authentication error. Please check your credentials and try again.")
except Exception as e:
    print(f"An error occurred: {e}")