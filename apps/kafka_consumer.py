#!/usr/bin/python3.10

from kafka import KafkaConsumer
from json import loads
import pymongo
import json

class KafkaToMongoDB:
    def __init__(self, kafka_topic, mongo_uri, database_name, collection_name):
        self.kafka_topic = kafka_topic
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name

    def initialize_consumer(self):
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )

    def initialize_mongo_client(self):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.database_name]

    def consume_and_insert(self):
        while True:
            data = next(self.consumer).value
            print(data)
            json_string = json.loads(data)
            collection = self.db[self.collection_name]
            try:
                inserted_id = collection.insert_one(json_string).inserted_id
                print("INSERT SUCCEEDED")
            except Exception as Error:
                print("INSERT FAILED")
                print(Error)

if __name__ == "__main__":
    kafka_topic = 'test_kafka'
    mongo_uri = "mongodb://root:example@localhost:27017/"
    database_name = 'iot_simulator'
    collection_name = 'temperature'  # Replace with your desired collection name

    kafka_to_mongodb = KafkaToMongoDB(kafka_topic, mongo_uri, database_name, collection_name)
    kafka_to_mongodb.initialize_consumer()
    kafka_to_mongodb.initialize_mongo_client()
    kafka_to_mongodb.consume_and_insert()
