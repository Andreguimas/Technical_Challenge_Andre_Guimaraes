from kafka import KafkaProducer
import fastavro
import io
import json
from bson.json_util import dumps

from pymongo import MongoClient
import time
from pymongo.cursor import CursorType


# Define o esquema Avro para os dados de cada cidade
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

# Function to connect to MongoDB and replace value
def drop_collection_mongodb(db_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]
    
    collection.drop()

# Função para serializar o registro (dicionário) usando o esquema Avro
def serialize_city_data(data):
    bytes_writer = io.BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema_avro, data)
    return bytes_writer.getvalue()

# Função para configurar a conexão com o MongoDB
def setup_mongodb_connection():
    client = MongoClient('localhost', 27017)
    db = client['technical_challenge_ubiwhere']
    collection = db['best_cities']
    return collection

# Função para processar o evento e enviar para o tópico Kafka
def process_event(event):
    city_data = json.loads(dumps(event))
    serialized_data = serialize_city_data(city_data)
    producer.send(topic_name, value=serialized_data)

# Configuração do produtor Kafka
kafka_broker = 'localhost:9092'  # Endereço do servidor Kafka
topic_name = 'city_data_topic'  # Nome do tópico Kafka

producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: v  # Não faz a serialização, já que os dados são bytes
)

# Configuração da conexão com o MongoDB
collection = setup_mongodb_connection()

# Lista para armazenar os IDs dos documentos já processados
processed_ids = []

#Drop cities collection
drop_collection_mongodb('sustainability', 'cities')

'''# Loop para capturar eventos em tempo real através do polling
while True:
    new_documents = collection.find({"_id": {"$nin": processed_ids}})

    for event in new_documents:
        process_event(event)
        processed_ids.append(event["_id"])

# O loop acima continuará fazendo o polling na coleção do MongoDB para verificar se há novos documentos adicionados e enviando-os para o tópico Kafka.
# Para interromper o loop, pressione 'Ctrl + C' no terminal onde o script está a ser executado.'''

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