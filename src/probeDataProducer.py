"""
Pushes Probe Archive Data to Kafka for previous day
"""

import requests, json
from kafka import KafkaProducer
from datetime import datetime, timedelta

import msgpack

from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType

class ProbeDataProducer():
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', acks=0,
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            batch_size=65536,linger_ms=4000,compression_type='gzip')

        self.topicName = "ihr_atlas_probe_archive"

        self.date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    def adjustConfig(self):    
        #To adjust configurations when the topic is just created
        admin = KafkaAdminClient()
        altered = admin.alter_configs([ConfigResource(ConfigResourceType.TOPIC,"ihr_atlas_probe_archive",{"retention.ms":1,"cleanup.policy":"compact"})])

    def start(self):
        link = "https://atlas.ripe.net/api/v2/probes/archive?day="+self.date

        data = requests.get(link).json()

        """
        with open("../data/probeArchives/2019-07-02.json") as myFile:
            data = json.loads(myFile.read())
        """

        filename = data["source_filename"]
        timestamp = data["snapshot_datetime"]
        data = data["results"]

        for record in data:
            #Push individual record to Kafka
            record["source_filename"] = filename
            record["snapshot_datetime"] = timestamp

            currentId = record["id"]

            timestampInMS = timestamp * 1000

            self.producer.send(self.topicName,key=bytes(str(currentId), "utf-8"),value=record,timestamp_ms=timestampInMS) 



