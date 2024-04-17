import pandas as pd
from pymongo import MongoClient
import json

# Connect to your MongoDB instance
client = MongoClient("<connection_string>")
db = client["<database_name>"]
collection = db["<collection_name>"]

# Read the Excel file
excel_file = pd.ExcelFile("<file_path>")
sheet_name = "<sheet_name>"
df = pd.read_excel(excel_file, sheet_name=sheet_name)

# Convert each row into JSON and insert into MongoDB
for index, row in df.iterrows():
    json_row = row.to_json(orient="records")[1:-1]  # Convert row to JSON
    data = json.loads(json_row)  # Load JSON string to dict
    collection.insert_one(data)  # Insert into MongoDB

print("Data loaded into MongoDB successfully.")