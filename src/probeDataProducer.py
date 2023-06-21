"""
Push Probe Archive Data to Kafka for one day
"""

import requests, json
from kafka import KafkaProducer
from datetime import datetime

import msgpack

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks=0,
    value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
    batch_size=65536,linger_ms=4000,compression_type='gzip')

topicName = "probeArchiveData"

#Takes too much time!

date = "2019-07-02"

link = "https://atlas.ripe.net/api/v2/probes/archive?day="+date

print("Url: ",link)

data = requests.get(link).json()


# with open("../data/probeArchives/2019-07-02.json") as myFile:
    # data = json.loads(myFile.read())

filename = data["source_filename"]
timestamp = data["snapshot_datetime"]
data = data["results"]

for record in data:
    #Push individual record to Kafka
    record["source_filename"] = filename
    record["snapshot_datetime"] = timestamp

    currentId = record["id"]

    timestampInMS = timestamp * 1000

    producer.send(topicName,key=bytes(str(currentId), "utf-8"),value=record,timestamp_ms=timestampInMS) 

