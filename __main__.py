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

    def run(self):
        self.initialize_consumer()
        self.initialize_mongo_client()

        while True:
            data = next(self.consumer).value.encode('utf-8')
            print(data)
            print(type(data))
            data = json.loads(data)
            collection = self.db[self.collection_name]
            try:
                inserted_id = collection.insert_one(data).inserted_id
                print("INSERT SUCCEEDED")
            except Exception as Error:
                print("INSERT FAILED")

if __name__ == "__main__":
    kafka_topic = 'test_kafka'
    mongo_uri = "mongodb://root:example@localhost:27017/"
    database_name = 'iot_simulator'
    collection_name = 'your_collection_name'  # Replace with your desired collection name

    kafka_to_mongodb = KafkaToMongoDB(kafka_topic, mongo_uri, database_name, collection_name)
    kafka_to_mongodb.run()
