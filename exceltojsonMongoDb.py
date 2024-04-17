import pandas as pd
from pymongo import MongoClient
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_excel_to_mongodb(file_path, sheet_name, connection_string, database_name, collection_name):
    """
    Load data from an Excel file into MongoDB.

    Parameters:
        file_path (str): The path to the Excel file.
        sheet_name (str): The name of the sheet in the Excel file.
        connection_string (str): The connection string for MongoDB.
        database_name (str): The name of the MongoDB database.
        collection_name (str): The name of the collection in MongoDB.
    """
    try:
        # Connect to MongoDB
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db[collection_name]

        # Read the Excel file
        excel_file = pd.ExcelFile(file_path)
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Convert each row into JSON and insert into MongoDB
        for index, row in df.iterrows():
            json_row = row.to_json(orient="records")[1:-1]  # Convert row to JSON
            data = json.loads(json_row)  # Load JSON string to dict
            collection.insert_one(data)  # Insert into MongoDB
        
        logging.info("Data loaded into MongoDB successfully.")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Usage
file_path = "<file_path>"
sheet_name = "<sheet_name>"
connection_string = "<connection_string>"
database_name = "<database_name>"
collection_name = "<collection_name>"

load_excel_to_mongodb(file_path, sheet_name, connection_string, database_name, collection_name)