import decrypt
import blob_writer

def decrypt_and_upload_blob(vault_url, public_key_secret_name, passphrase_secret_name, client_id,
                            gpg_file_path, account_url, container_name, blob_name):
    """
    Decrypt a file using GPG keys stored in Azure Key Vault and upload it to Azure Blob Storage.

    Parameters:
        vault_url (str): The URL of the Azure Key Vault.
        public_key_secret_name (str): The name of the secret containing the GPG public key.
        passphrase_secret_name (str): The name of the secret containing the passphrase.
        client_id (str): The client ID of the User-Assigned Managed Identity.
        gpg_file_path (str): The path to the encrypted GPG file.
        account_url (str): The URL of the Azure Blob Storage account.
        container_name (str): The name of the container in Azure Blob Storage.
        blob_name (str): The name of the blob to which the decrypted data will be written.
    """
    try:
        # Decrypt the GPG file
        decrypted_stream = decrypt.decrypt_gpg_file_from_keyvault(
            vault_url, public_key_secret_name, passphrase_secret_name, client_id, gpg_file_path
        )

        # Upload decrypted data to Azure Blob Storage
        blob_writer.write_stream_to_blob(decrypted_stream, account_url, container_name, blob_name)
    except Exception as e:
        print(f"Error: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Set your parameters here
    vault_url = "YOUR_VAULT_URL"
    public_key_secret_name = "PUBLIC_KEY_SECRET_NAME"
    passphrase_secret_name = "PASSPHRASE_SECRET_NAME"
    client_id = "YOUR_CLIENT_ID"
    gpg_file_path = "PATH_TO_ENCRYPTED_FILE"
    account_url = "YOUR_BLOB_ACCOUNT_URL"
    container_name = "YOUR_CONTAINER_NAME"
    blob_name = "YOUR_BLOB_NAME"

    # Decrypt and upload blob
    decrypt_and_upload_blob(vault_url, public_key_secret_name, passphrase_secret_name, client_id,
                            gpg_file_path, account_url, container_name, blob_name)