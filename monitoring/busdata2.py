from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time

#READ COORDINATES FROM GEOJSON
input_file = open('./data/bus2.json')
json_array = json.load(input_file)
coordinates = json_array['features'][0]['geometry']['coordinates']

#GENERATE UUID
def generate_uuid():
    return uuid.uuid4()

#KAFKA PRODUCER
client = KafkaClient(hosts="localhost:9092")
topic = client.topics['geodata_final123']
producer = topic.get_sync_producer()

# #CONSTRUCT MESSAGE AND SEND IT TO KAFKA
# data = {}
# data['busline'] = '00002'

# def generate_checkpoint(coordinates):
#     i = 0
#     while i < len(coordinates):
#         data['key'] = data['busline'] + '_' + str(generate_uuid())
#         data['timestamp'] = str(datetime.utcnow())
#         data['latitude'] = coordinates[i][1]
#         data['longitude'] = coordinates[i][0]
#         message = json.dumps(data)
#         print(message)
#         producer.produce(message.encode('ascii'))
#         time.sleep(1)

#         #if bus reaches last coordinate, start from beginning
#         if i == len(coordinates)-1:
#             i = 0
#         else:
#             i += 1

# generate_checkpoint(coordinates)


def create_kafka_producer():
    """Initialize and return a Kafka producer."""
    client = KafkaClient(hosts="localhost:9092")
    topic = client.topics[b'bus-coordinates']
    return topic.get_sync_producer()

def construct_message(busline, coordinates, index):
    """Construct a message with the busline data and checkpoint coordinates."""
    return {
        'key': f"{busline}_{str(uuid4())}",
        'timestamp': str(datetime.utcnow()),
        'latitude': coordinates[index][1],
        'longitude': coordinates[index][0],
        'busline': busline
    }

def generate_checkpoints(busline, coordinates, producer, interval=1):
    """Generate checkpoints for the busline and send messages to Kafka."""
    index = 0
    while True:
        message = construct_message(busline, coordinates, index)
        producer.produce(json.dumps(message).encode('ascii'))
        print(json.dumps(message))
        
        time.sleep(interval)
        # Loop back to the start after reaching the last coordinate
        index = (index + 1) % len(coordinates)

if __name__ == 
