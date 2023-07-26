from kafka import KafkaConsumer
import fastavro
import io
import json
from bson.json_util import dumps
from pymongo import MongoClient

import pandas as pd

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

# Function to insert city data into MongoDB table
def insert_city_data_into_mongodb(city_data, db_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_one(city_data)

# Function to insert city data into MongoDB table
def insert_data_into_mongodb(city_data, db_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(city_data)

# Function to connect to MongoDB and retrieve the cities data
def get_city_data_from_mongodb():
    client = MongoClient('localhost', 27017)
    db = client['sustainability']
    collection = db['cities']
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
        average_sustainability = total_sustainability / len(cities)
       # print(f"Average sustainability index for continent {continent}: {average_sustainability}")

        # Check if this continent has a higher average sustainability index than the current best
        if average_sustainability < best_sustainability:
            best_sustainability = average_sustainability
            best_continent = continent

        data_list.append({"Continent": continent, "AverageRanking": average_sustainability})

    # Add the "best" metric to the data list
    data_list.append({"Best Continent": best_continent, "AverageRanking": best_sustainability})

    drop_collection_mongodb('sustainability', 'continent_metrics')
    insert_data_into_mongodb(data_list, 'sustainability', 'continent_metrics')

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
        average_sustainability = total_sustainability / len(cities)
        #print(f"Average sustainability index for country {country}: {average_sustainability}")

        # Check if this continent has a higher average sustainability index than the current best
        if average_sustainability < best_sustainability:
            best_sustainability = average_sustainability
            best_country = country
        
        data_list.append({"Country": country, "AverageRanking": average_sustainability})
    
    # Add the "best" metric to the data list
    data_list.append({"Best Country": best_country, "AverageRanking": best_sustainability})

    drop_collection_mongodb('sustainability', 'country_metrics')
    insert_data_into_mongodb(data_list, 'sustainability', 'country_metrics')

#Train the machine learning model
def predict_most_positive_feature():
    # Get the cities data from MongoDB
    city_data_list = get_city_data_from_mongodb()

    #Create Dataframe
    df = pd.DataFrame(city_data_list)

    from sklearn.model_selection import train_test_split

    # Separar as features (indicadores) e a variável de destino (pontuação de sustentabilidade)
    X = df.drop(['_id','city', 'Country', 'Continent', 'Overall'], axis=1)
    y = df['Overall']

    # Dividir os dados em conjuntos de treinamento e teste (80% treinamento, 20% teste)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    from sklearn.linear_model import LinearRegression
    # Escolher o modelo de regressão (neste caso, usaremos a regressão linear)
    model = LinearRegression()

    # Treinar o modelo usando os dados de treinamento
    model.fit(X_train, y_train)

    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    # Fazer previsões usando o conjunto de teste
    y_pred = model.predict(X_test)

    # Calcular as métricas de avaliação
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print("Mean Absolute Error:", mae)
    print("Mean Squared Error:", mse)
    print("R-squared:", r2)

    # Obter os coeficientes do modelo (os pesos atribuídos a cada indicador)
    coeficients = model.coef_

    # Criar um dicionário para associar os coeficientes aos nomes das features
    features_coef = dict(zip(X.columns, coeficients))

    # Identificar o indicador com maior impacto positivo e negativo
    most_positive_feature = max(features_coef, key=features_coef.get)
    most_negative_feature = min(features_coef, key=features_coef.get)

    print("Indicador com maior impacto positivo:", most_positive_feature)
    print("Indicador com maior impacto negativo:", most_negative_feature)

    return most_positive_feature, most_negative_feature

# Loop to consume and process new data in real-time
for message in consumer:
    city_data = message.value

    # Insert the city data into MongoDB
    insert_city_data_into_mongodb(city_data, 'sustainability', 'cities')

    # Get the cities data from MongoDB
    city_data_list = get_city_data_from_mongodb()

    # Calculate continent sustainability based on the ranking average (lower the best) and insert on db
    calculate_continent_sustainability_and_insert_on_db(city_data_list)

    # Calculate country sustainability based on the ranking average (lower the best) and insert on db
    calculate_country_sustainability_and_insert_on_db(city_data_list)

    if city_data['Overall'] == 100:
        consumer.close()


most_positive_factor,most_negative_factor =  predict_most_positive_feature()

# Insert the city data into MongoDB
insert_city_data_into_mongodb({"Most_Positive_Factor": most_positive_factor, "Most_Negative_Factor": most_negative_factor}, 'sustainability', 'cities')

