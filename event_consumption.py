# Import the necessary libraries
from kafka import KafkaConsumer
import fastavro
import io
import json
from bson.json_util import dumps
from pymongo import MongoClient

import pandas as pd
import joblib

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

# Function to deserialize the data using the Avro schema
def deserialize_city_data(data):
    bytes_reader = io.BytesIO(data)
    reader = fastavro.schemaless_reader(bytes_reader, schema_avro)
    return dict(reader)

# Function to insert city data into MongoDB collection
def insert_city_data_into_mongodb(city_data, db_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_one(city_data)

# Function to insert city data into MongoDB collection
def insert_data_into_mongodb(city_data, db_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(city_data)

# Function to connect to MongoDB and retrieve the cities data
def get_data_from_mongodb(database_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[database_name]
    collection = db[collection_name]
    return list(collection.find())

# Function to connect to MongoDB and replace value
def drop_collection_mongodb(db_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]
    
    collection.drop()

# Kafka consumer configuration
kafka_broker = 'localhost:9092'  # Kafka broker address
topic_name = 'city_data_topic'  # Kafka topic name

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=kafka_broker,
    value_deserializer=lambda v: deserialize_city_data(v)
)

# Function to calculate the average sustainability index for a continent
def calculate_continent_sustainability_and_insert_on_db(cities_data):

    # List to store city data for each continent
    continent_cities_dict = {}

    # Dictionary for data insertion
    data_list = []

    for city_data in cities_data:

        # Group city data by continent
        continent = city_data['Continent']
        if continent not in continent_cities_dict:
            continent_cities_dict[continent] = []
        continent_cities_dict[continent].append(city_data)

    # Calculate and print the average sustainability index for each continent
    best_continent = None
    best_sustainability = 100

    # Calculate and print the average sustainability index for each continent
    for continent, cities in continent_cities_dict.items():
        total_sustainability = sum(city_data['Overall'] for city_data in cities)
        total_people_points = sum(city_data['People'] for city_data in cities)
        total_planet_points = sum(city_data['Planet'] for city_data in cities)
        total_profit_points = sum(city_data['Profit'] for city_data in cities)
        average_sustainability = total_sustainability / len(cities)
        average_people_points = total_people_points / len(cities)
        average_planet_points = total_planet_points / len(cities)
        average_profit_points = total_profit_points / len(cities)
       # print(f"Average sustainability index for continent {continent}: {average_sustainability}")

        # Check if this continent has a higher average sustainability index than the current best
        if average_sustainability < best_sustainability:
            best_sustainability = average_sustainability
            best_continent = continent

        data_list.append({"Continent": continent, "AverageRanking": average_sustainability, "Average_People_Points": average_people_points, "Average_Planet_Points": average_planet_points, "Average_Profit_Points": average_profit_points})

    # Add the "best" metric to the data list
    data_list.append({"Best Continent": best_continent, "AverageRanking": best_sustainability})

    drop_collection_mongodb('sustainability', 'continent_metrics')
    insert_data_into_mongodb(data_list, 'sustainability', 'continent_metrics')
    return data_list

# Function to calculate the average sustainability index for a country
def calculate_country_sustainability_and_insert_on_db(cities_data):

    # Dictionary to store city data for each country
    country_cities_dict = {}

    # Dictionary for data insertion
    data_list = []

    for city_data in cities_data:

        # Group city data by country
        country = city_data['Country']
        if country not in country_cities_dict:
            country_cities_dict[country] = []
        country_cities_dict[country].append(city_data)

    # Calculate and print the average sustainability index for each continent
    best_country = None
    best_sustainability = 100

    # Calculate and print the average sustainability index for each country
    for country, cities in country_cities_dict.items():
        total_sustainability = sum(city_data['Overall'] for city_data in cities)
        total_people_points = sum(city_data['People'] for city_data in cities)
        total_planet_points = sum(city_data['Planet'] for city_data in cities)
        total_profit_points = sum(city_data['Profit'] for city_data in cities)
        average_sustainability = total_sustainability / len(cities)
        average_people_points = total_people_points / len(cities)
        average_planet_points = total_planet_points / len(cities)
        average_profit_points = total_profit_points / len(cities)
        #print(f"Average sustainability index for country {country}: {average_sustainability}")

        # Check if this continent has a higher average sustainability index than the current best
        if average_sustainability < best_sustainability:
            best_sustainability = average_sustainability
            best_country = country
        
        data_list.append({"Country": country, "AverageRanking": average_sustainability, "Average_People_Points": average_people_points, "Average_Planet_Points": average_planet_points, "Average_Profit_Points": average_profit_points})
    
    # Add the "best" metric to the data list
    data_list.append({"Best Country": best_country, "AverageRanking": best_sustainability})

    drop_collection_mongodb('sustainability', 'country_metrics')
    insert_data_into_mongodb(data_list, 'sustainability', 'country_metrics')
    return data_list

# Train the machine learning model and predict the most positive feature
def predict_most_positive_feature(data, model_filename, columns_to_drop):
    # Create DataFrame
    df = pd.DataFrame(data)

    # Separate the features (indicators) and the target variable (sustainability score)
    df = df.drop(columns_to_drop, axis=1)

    # Load the model from the file
    loaded_model = joblib.load(model_filename)

    # Make predictions
    predictions = loaded_model.predict(df)

    # Get the coefficients of the model (the weights assigned to each indicator)
    coefficients = loaded_model.coef_

    # Create a dictionary to associate the coefficients with the feature names
    features_coef = dict(zip(df.columns, coefficients))

    # Identify the indicator with the highest positive impact and the highest negative impact
    most_positive_feature = max(features_coef, key=features_coef.get)
    most_negative_feature = min(features_coef, key=features_coef.get)

    print("Indicator with the highest positive impact:", most_positive_feature)
    print("Indicator with the highest negative impact:", most_negative_feature)

    return most_positive_feature, most_negative_feature

# Loop to consume and process new data in real-time
for message in consumer:
    city_data = message.value

    # Insert the city data into MongoDB collection
    insert_city_data_into_mongodb(city_data, 'sustainability', 'cities')

    # Get the cities data from MongoDB
    city_data_list = get_data_from_mongodb('sustainability', 'cities')

    # Calculate continent sustainability based on the ranking average (lower the best) and insert on db
    continent_data_list = calculate_continent_sustainability_and_insert_on_db(city_data_list)

    # Calculate country sustainability based on the ranking average (lower the best) and insert on db
    country_data_list = calculate_country_sustainability_and_insert_on_db(city_data_list)

    if city_data['Overall'] == 100:
        consumer.close()

# Predict the most positive and negative features and insert them into the 'cities' collection
most_positive_factor, most_negative_factor = predict_most_positive_feature(city_data_list, 'pretrained_model/linear_regression_model.joblib', ['_id','city', 'Country', 'Continent', 'Overall'])

insert_city_data_into_mongodb({"Most_Positive_Factor": most_positive_factor, "Most_Negative_Factor": most_negative_factor}, 'sustainability', 'cities')