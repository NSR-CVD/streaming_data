from kafka import KafkaConsumer
from json import loads
import pymongo
import json

consumer = KafkaConsumer(
    'test_kafka',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

# Replace the following with your MongoDB connection string
mongo_uri = "mongodb://root:example@localhost:27017/"

client = pymongo.MongoClient(mongo_uri)
db = client['iot_simulator']
print(db)
# print(collection)
# print(db)
while True:
    data = next(consumer).value.encode('utf-8')
    print(data)
    print(type(data))
    # print(data.value)
    data = json.loads(data)
    # print(type(data))
    collection = db.collection_name
    try:
        inserted_id = collection.insert_one(data).inserted_id
        print("INSERT SUCCEEDDED")
    except: 
        print("INSERT FAILED")