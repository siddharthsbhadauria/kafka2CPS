import decrypt
from azure.storage.blob import BlobServiceClient
from azure.identity import ManagedIdentityCredential

def upload_to_blob_storage(decrypted_data, container_name, blob_name):
    """Upload decrypted data to Azure Blob Storage."""
    try:
        # Authenticate with User-Assigned Managed Identity
        credential = ManagedIdentityCredential()

        # Create a BlobServiceClient using a private endpoint
        blob_service_client = BlobServiceClient(
            account_url="YOUR_PRIVATE_ENDPOINT_URL",
            credential=credential
        )

        # Get a container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Upload decrypted data as a blob
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(decrypted_data)
        
        print("File uploaded successfully.")
    except Exception as e:
        print(f"Error uploading file to Azure Blob Storage: {str(e)}")

if __name__ == "__main__":
    try:
        # Get parameters from the new module
        vault_url = "YOUR_VAULT_URL"
        public_key_secret_name = "PUBLIC_KEY_SECRET_NAME"
        passphrase_secret_name = "PASSPHRASE_SECRET_NAME"
        client_id = "YOUR_CLIENT_ID"
        gpg_file_path = "PATH_TO_ENCRYPTED_FILE"
        container_name = "YOUR_CONTAINER_NAME"
        blob_name = "YOUR_BLOB_NAME"

        # Decrypt the GPG file
        decrypted_stream = decrypt.decrypt_gpg_file_from_keyvault(
            vault_url,
            public_key_secret_name,
            passphrase_secret_name,
            client_id,
            gpg_file_path
        )

        # Upload decrypted data to Azure Blob Storage
        upload_to_blob_storage(decrypted_stream, container_name, blob_name)
    except Exception as e:
        print(f"Error: {str(e)}")