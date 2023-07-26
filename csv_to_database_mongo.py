import argparse
import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
def connect_to_mongodb():
    client = MongoClient('mongodb://localhost:27017/')  # MongoDB connection string
    db = client['technical_challenge_ubiwhere']  # technical_challenge_ubiwhere database
    return db

# Read data from CSV and insert into MongoDB collection (e.g., best_cities)
def insert_data_to_mongodb(csv_file, collection_name):
    # Read data from CSV into a pandas DataFrame
    data_df = pd.read_csv(csv_file)

    # Convert DataFrame to a list of dictionaries for easy insertion
    data_list = data_df.to_dict(orient='records')

    # Connect to MongoDB
    db = connect_to_mongodb()

    # Insert data into the specified MongoDB collection (e.g., best_cities)
    collection = db[collection_name]
    result = collection.insert_many(data_list)

    # Print the number of documents inserted
    print(f"{len(result.inserted_ids)} documents inserted.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Insert data from a CSV file into MongoDB.')
    parser.add_argument('csv_file_path', type=str, help='Path to the CSV file')
    parser.add_argument('collection_name', type=str, help='Name of the MongoDB collection')
    args = parser.parse_args()

    csv_file_path = args.csv_file_path
    collection_name = args.collection_name

    insert_data_to_mongodb(csv_file_path, collection_name)