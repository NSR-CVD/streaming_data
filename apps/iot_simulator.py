#!/usr/bin/python3.10

from kafka import KafkaProducer
from json import dumps
import sys
import datetime
import random
import re

class IoTDataGenerator:
    def __init__(self, kafka_topic, num_msgs=1):
        self.kafka_topic = kafka_topic
        self.num_msgs = num_msgs
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        self.device_state_map = {}
        self.temp_base = {
            'WA': 48.3, 'DE': 55.3, 'DC': 58.5, 'WI': 43.1,
            # ... (other state temperature values)
        }
        self.current_temp = {}
        self.guid_base = "0-ZZZ12345678-"
        self.destination = "0-AAA12345678"
        self.format_str = "urn:example:sensor:temp"
        self.letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

    def generate_iot_data(self):
        for counter in range(0, self.num_msgs):
            rand_num = str(random.randrange(0, 9)) + str(random.randrange(0, 9))
            rand_letter = random.choice(self.letters)
            temp_init_weight = random.uniform(-5, 5)
            temp_delta = random.uniform(-1, 1)

            guid = self.guid_base + rand_num + rand_letter
            state = random.choice(list(self.temp_base.keys()))

            if guid not in self.device_state_map:
                self.device_state_map[guid] = state
                self.current_temp[guid] = self.temp_base[state] + temp_init_weight
            elif self.device_state_map[guid] != state:
                state = self.device_state_map[guid]

            temperature = self.current_temp[guid] + temp_delta
            self.current_temp[guid] = temperature
            today = datetime.datetime.today()
            datestr = today.isoformat()

            iot_data = {
                "guid": guid,
                "destination": self.destination,
                "state": state,
                "eventTime": datestr + "Z",
                "payload": {
                    "format": self.format_str,
                    "data": {
                        "temperature": round(temperature, 1)
                    }
                }
            }

            self.producer.send(self.kafka_topic, value=iot_data)
            print(iot_data)

if __name__ == "__main__":
    kafka_topic = 'test_kafka'
    num_msgs = 1 if len(sys.argv) <= 1 else int(sys.argv[1])

    iot_data_generator = IoTDataGenerator(kafka_topic, num_msgs)
    iot_data_generator.generate_iot_data()
