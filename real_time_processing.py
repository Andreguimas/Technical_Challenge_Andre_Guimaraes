# Import necessary libraries
from kafka import KafkaProducer
import fastavro
import io
import json
from bson.json_util import dumps

from pymongo import MongoClient
import time
from pymongo.cursor import CursorType

# Define the Avro schema for the city data
schema_avro = {
    "type": "record",
    "name": "CityData",
    "fields": [
        {"name": "city", "type": "string"},
        {"name": "People", "type": "int"},
        {"name": "Planet", "type": "int"},
        {"name": "Profit", "type": "int"},
        {"name": "Overall", "type": "int"},
        {"name": "Country", "type": "string"},
        {"name": "Continent", "type": "string"}
    ]
}

# Function to connect to MongoDB and drop a collection
def drop_collection_mongodb(db_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]
    
    collection.drop()

# Function to serialize a record (dictionary) using the Avro schema
def serialize_city_data(data):
    bytes_writer = io.BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema_avro, data)
    return bytes_writer.getvalue()

# Function to set up the MongoDB connection
def setup_mongodb_connection():
    client = MongoClient('localhost', 27017)
    db = client['technical_challenge_ubiwhere']
    collection = db['best_cities']
    return collection

# Function to process an event and send it to the Kafka topic
def process_event(event):
    city_data = json.loads(dumps(event))
    serialized_data = serialize_city_data(city_data)
    producer.send(topic_name, value=serialized_data)

# Kafka producer configuration
kafka_broker = 'localhost:9092'  # Kafka broker address
topic_name = 'city_data_topic'  # Kafka topic name

producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: v  # No serialization is performed, as the data is already in bytes
)

# MongoDB connection setup
collection = setup_mongodb_connection()

# List to store the IDs of the processed documents
processed_ids = []

# Drop the 'cities' collection
drop_collection_mongodb('sustainability', 'cities')

# Function to process all events and send them to the Kafka topic
def process_all_events():
    new_documents = collection.find()

    for event in new_documents:
        process_event(event)
        processed_ids.append(event["_id"])

# Process all events and send them to Kafka
process_all_events()

# Close the Kafka producer
producer.close()