"""
Pushes Atlas Live Events to Kafka indefinitely
"""

from kafka import KafkaProducer
from datetime import datetime
import time
import json
from ripe.atlas.cousteau import AtlasResultsRequest
import msgpack

class EventProducer():
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', acks=0,
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            batch_size=65536,linger_ms=4000,compression_type='gzip')

        self.topicName = "ihr_atlas_live"

    def startLive(self):
        WINDOW = 60
        currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
        while True:
            try:
                kwargs = {
                    "msm_id": 7000,
                    "start": datetime.utcfromtimestamp(currentTS-WINDOW),
                    "stop": datetime.utcfromtimestamp(currentTS),
                }

                is_success, results = AtlasResultsRequest(**kwargs).create()
                if is_success:
                    for ent in results:
                        timestamp = ent["timestamp"]
                        timestamp = timestamp*1000      #convert to milliseconds
                        self.producer.send(self.topicName,ent,timestamp_ms=timestamp)
                        print("Record pushed")
                else:
                    print("Fetch Failed!")

                time.sleep(WINDOW)
                currentTS += (WINDOW + 1)
            except Exception as e:
                print("Error: ",e)

    def startPeriod(self,startTS,endTS):
        kwargs = {
            "msm_id": 7000,
            "start": datetime.utcfromtimestamp(startTS),
            "stop": datetime.utcfromtimestamp(endTS),
        }

        is_success, results = AtlasResultsRequest(**kwargs).create()

        if is_success:
            for ent in results:
                timestamp = ent["timestamp"]
                timestamp = timestamp*1000      #convert to milliseconds
                self.producer.send(self.topicName,ent,timestamp_ms=timestamp)
                print("Record pushed -- ",timestamp)
        else:
            print("Fetch Failed!")




#EXAMPLE
EventProducer().startPeriod(1553385600,1553644800)
