import pandas as pd
from presidio_analyzer import AnalyzerEngine
from pymongo import MongoClient
from azure.identity import ClientSecretCredential

# Step 1: Read the CSV data and limit to the first 2000 rows
def read_csv_subset(csv_file, row_limit):
    data = pd.read_csv(csv_file)
    subset = data.iloc[:row_limit]
    return subset

# Step 2: Identify PII columns using Presidio
def identify_pii_columns(data):
    analyzer = AnalyzerEngine()  # Create the analyzer engine
    pii_columns = set()  # Store columns identified as PII
    
    # Analyze each column to identify PII
    for col in data.columns:
        text = " ".join(data[col].astype(str).tolist())  # Convert the column to text
        results = analyzer.analyze(text=text, language='en')  # Analyze for PII
        
        # If any PII detection has a significant score (e.g., > 0.5)
        if any(result.score > 0.5 for result in results):
            pii_columns.add(col)
    
    return pii_columns

# Step 3: Connect to Cosmos DB using a service principal
def connect_to_cosmos(tenant_id, client_id, client_secret, cosmos_endpoint):
    # Establish service principal credentials
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )

    # Get a token for Cosmos DB
    token = credential.get_token("https://cosmos.azure.com/.default")

    # Connect to Cosmos DB
    client = MongoClient(cosmos_endpoint, authMechanism="SCRAM-SHA-1", username="your_db_user", password=token.token)

    return client

# Step 4: Fetch expected PII columns from Cosmos DB
def get_expected_pii_columns(client, table_name):
    db = client['your_database']  # Replace with your Cosmos DB database name
    collection = db['csv_schemas']  # Replace with your Cosmos DB collection name
    
    # Fetch expected schema based on the table name
    schema_docs = collection.find({'table_name': table_name, 'PII': 'Y'})
    
    # Extract the column names marked as PII
    expected_pii_columns = {doc['column_name'] for doc in schema_docs}
    
    return expected_pii_columns

# Step 5: Compare detected PII with expected PII and raise error if discrepancies exist
def compare_with_expected_pii(detected_pii, expected_pii):
    extra_pii_columns = detected_pii - expected_pii
    
    if extra_pii_columns:
        raise ValueError(f"Unexpected PII columns found: {extra_pii_columns}")
    else:
        return "PII check passed. No unexpected PII columns found."

# Constants for Cosmos DB connection
tenant_id = "your_tenant_id"  # Your Azure AD tenant ID
client_id = "your_client_id"  # Your Azure AD client ID
client_secret = "your_client_secret"  # Your Azure AD client secret
cosmos_endpoint = "your_cosmos_endpoint"  # Your Cosmos DB endpoint

# CSV file and subset
csv_file = "your_file.csv"  # Replace with your CSV file name
subset_data = read_csv_subset(csv_file, 2000)  # Read the first 2000 rows

# Identify PII columns in the CSV
pii_columns_found = identify_pii_columns(subset_data)
print("PII Columns Identified:", pii_columns_found)

# Extract table name from the CSV file name
table_name = csv_file.split('_')[2]  # Get the third part of the filename separated by underscore

# Connect to Cosmos DB and get expected PII columns
client = connect_to_cosmos(tenant_id, client_id, client_secret, cosmos_endpoint)
expected_pii_columns = get_expected_pii_columns(client, table_name)

# Compare detected PII with expected PII and raise an error if unexpected columns are found
result = compare_with_expected_pii(pii_columns_found, expected_pii_columns)

print(result)  # Output the comparison result