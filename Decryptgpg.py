import logging
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
import gnupg
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_secret_from_keyvault(vault_url, secret_name, credential):
    """Retrieve secret value from Azure Key Vault."""
    try:
        secret_client = SecretClient(vault_url=vault_url, credential=credential)
        secret_value = secret_client.get_secret(secret_name).value
        return secret_value
    except Exception as e:
        logger.error(f"Failed to retrieve secret '{secret_name}' from Azure Key Vault: {str(e)}")
        raise Exception(f"Failed to retrieve secret '{secret_name}' from Azure Key Vault: {str(e)}")

def decrypt_gpg_file_in_memory(file_content, passphrase):
    """Decrypt GPG file content in memory."""
    try:
        gpg = gnupg.GPG()
        decrypted_data = gpg.decrypt_file(file_content, passphrase=passphrase)
        decrypted_stream = io.BytesIO(decrypted_data.data)
        return decrypted_stream
    except Exception as e:
        logger.error(f"Failed to decrypt GPG file content: {str(e)}")
        raise Exception(f"Failed to decrypt GPG file content: {str(e)}")

def decrypt_gpg_file_from_keyvault(vault_url, secret_name_public_key, secret_name_passphrase, passphrase_client_id, gpg_file_path):
    """Decrypt GPG file stored in Azure Key Vault."""
    try:
        # Authenticate with User-Assigned Managed Identity
        credential = ManagedIdentityCredential(client_id=passphrase_client_id)

        # Get public key and passphrase from Azure Key Vault
        public_key = get_secret_from_keyvault(vault_url, secret_name_public_key, credential)
        passphrase = get_secret_from_keyvault(vault_url, secret_name_passphrase, credential)

        # Decrypt GPG file
        with open(gpg_file_path, 'rb') as f:
            decrypted_stream = decrypt_gpg_file_in_memory(f, passphrase)

        return decrypted_stream
    except Exception as e:
        logger.error(f"Failed to decrypt GPG file from Azure Key Vault: {str(e)}")
        raise Exception(f"Failed to decrypt GPG file from Azure Key Vault: {str(e)}")

# Example usage
try:
    decrypted_stream = decrypt_gpg_file_from_keyvault(
        "https://your-keyvault-name.vault.azure.net/",
        "public-key-secret-name",
        "passphrase-secret-name",
        "your-client-id",
        "path/to/encrypted/file.gpg"
    )

    # Now you can use decrypted_stream to read the decrypted data as a stream in memory
    # For example:
    # decrypted_data = decrypted_stream.read()
    # print(decrypted_data)
except Exception as e:
    logger.error(f"Error: {str(e)}")