import pandas as pd
from presidio_analyzer import AnalyzerEngine
from pymongo import MongoClient
from azure.identity import ClientSecretCredential
import logging

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO  # Set the logging level to INFO or DEBUG for more verbosity
)

# Custom exception class for Cosmos DB errors
class CosmosDBError(Exception):
    pass

# Function to read the CSV file and return a subset (with error handling and logging)
def read_csv_subset(csv_file, row_limit):
    try:
        data = pd.read_csv(csv_file)
        subset = data.iloc[:row_limit]
        return subset
    except FileNotFoundError:
        logging.error(f"File '{csv_file}' not found.")
        raise FileNotFoundError(f"File '{csv_file}' not found.")
    except Exception as e:
        logging.error(f"Error reading CSV: {e}")
        raise e

# Function to identify PII columns using Presidio
def identify_pii_columns(data):
    try:
        analyzer = AnalyzerEngine()  # Create the Presidio analyzer engine
        pii_columns = set()  # Store columns identified as PII
        
        # Analyze each column to identify PII
        for col in data.columns:
            text = " ".join(data[col].astype(str).tolist())  # Convert the column to text
            results = analyzer.analyze(text=text, language='en')  # Analyze for PII
            
            if any(result.score > 0.5 for result in results):  # Significant PII score
                pii_columns.add(col)
        
        return pii_columns
    except Exception as e:
        logging.error(f"Error identifying PII: {e}")
        raise e

# Function to connect to Cosmos DB using a service principal
def connect_to_cosmos(tenant_id, client_id, client_secret, cosmos_endpoint):
    try:
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        
        token = credential.get_token("https://cosmos.azure.com/.default")
        
        client = MongoClient(cosmos_endpoint, authMechanism="SCRAM-SHA-1", username="your_db_user", password=token.token)
        
        return client
    except Exception as e:
        logging.error(f"Error connecting to Cosmos DB: {e}")
        raise CosmosDBError(f"Could not connect to Cosmos DB: {e}")

# Function to fetch expected PII columns from Cosmos DB
def get_expected_pii_columns(client, table_name):
    try:
        db = client['your_database']  # Replace with your Cosmos DB database name
        collection = db['csv_schemas']  # Replace with your Cosmos DB collection name
        
        # Fetch expected schema based on the table name
        schema_docs = collection.find({'table_name': table_name, 'PII': 'Y'})
        
        expected_pii_columns = {doc['column_name'] for doc in schema_docs}
        
        return expected_pii_columns
    except Exception as e:
        logging.error(f"Error fetching expected PII columns: {e}")
        raise CosmosDBError(f"Could not fetch expected PII columns: {e}")

# Function to compare detected PII with expected PII and raise an exception if discrepancies exist
def compare_with_expected_pii(detected_pii, expected_pii):
    extra_pii_columns = detected_pii - expected_pii
    
    if extra_pii_columns:
        raise ValueError(f"Unexpected PII columns found: {extra_pii_columns}")
    else:
        return "PII check passed. No unexpected PII columns found."

# Constants for Cosmos DB connection and service principal
tenant_id = "your_tenant_id"  # Your Azure AD tenant ID
client_id = "your_client_id"  # Your Azure AD client ID
client_secret = "your_client_secret"  # Your Azure AD client secret
cosmos_endpoint = "your_cosmos_db_endpoint"  # Your Cosmos DB endpoint

# CSV file and subset
csv_file = "your_file.csv"  # Replace with your CSV file name
subset_data = read_csv_subset(csv_file, 2000)  # Read the first 2000 rows

# Identify PII columns in the CSV
try:
    pii_columns_found = identify_pii_columns(subset_data)
    logging.info(f"PII Columns Identified: {pii_columns_found}")
except Exception as e:
    logging.error("Failed to identify PII columns.")
    raise

# Extract table name from the CSV file name
table_name = csv_file.split('_')[2]  # Get the third part of the filename separated by underscores

# Connect to Cosmos DB and get expected PII columns
try:
    client = connect_to_cosmos(tenant_id, client_id, client_secret, cosmos_endpoint)
    expected_pii_columns = get_expected_pii_columns(client, table_name)
except CosmosDBError as e:
    logging.error("Error with Cosmos DB operations.")
    raise

# Compare detected PII with expected PII and raise an exception if unexpected columns are found
try:
    result = compare_with_expected_pii(pii_columns_found, expected_pii_columns)
    logging.info(result)  # Output the comparison result
except ValueError as ve:
    logging.error(f"Validation failed: {ve}")
    raise