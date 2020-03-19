"""
Pushes Probe Archive Data to Kafka for previous day
"""

import requests, json
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from datetime import datetime, timedelta
import reverse_geocoder as rg
from collections import defaultdict
import msgpack

from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType

class ProbeDataProducer():
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', acks=0,
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            batch_size=65536,linger_ms=4000,compression_type='gzip')

        self.topicName = "ihr_atlas_probe_archive"

        self.date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

        # Look for probe that are constantly disconnecting
        self.disco_count = defaultdict(lambda: defaultdict(int))
        self.estimateProbeNoise()

    def estimateProbeNoise(self, eventTopicName="ihr_atlas_probe_discolog"):

        consumer = KafkaConsumer(
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda v: msgpack.unpackb(v, raw=False)
                )

        topicPartition = TopicPartition(eventTopicName,0)
        yesterday = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())*1000
        yesterday -= 24*60*60*1000
        oneWeekAgo = yesterday - 7*24*60*60*1000
        consumer.assign([topicPartition])
        offsets = consumer.offsets_for_times({topicPartition:oneWeekAgo})
        while offsets[topicPartition] is None:
            #recheck after 10 seconds
            consumer.poll(10000)
            offsets = consumer.offsets_for_times({topicPartition:oneWeekAgo})

        theOffset = offsets[topicPartition].offset
        consumer.seek(topicPartition,theOffset)

        for message in consumer:
            msg = message.value
            # Stoping condition
            if msg['timestamp'] > yesterday/1000:
                break

            # Ignore old events
            if msg['timestamp'] < oneWeekAgo/1000:
                continue 

            if msg['event'] == 'disconnect':
                self.disco_count[msg['prb_id']][int(msg['timestamp'] / (8*3600))] += 1


    def adjustConfig(self):    
        #To adjust configurations when the topic is just created
        admin = KafkaAdminClient()
        altered = admin.alter_configs([ConfigResource(ConfigResourceType.TOPIC,"ihr_atlas_probe_archive",{"retention.ms":1,"cleanup.policy":"compact"})])

    def augmentWithLocation(self,record):
        probeLong = record["geometry"]["coordinates"][0]
        probeLat = record["geometry"]["coordinates"][1]

        if (not probeLat) or (not probeLong):
            lowPrecisionLoc = None
            highPrecisionLoc = None
            adminCoordinates = None
        else:
            probeAddress = rg.search((float(probeLat),float(probeLong)))[0]

            admin1 = probeAddress["admin1"]
            admin2 = probeAddress["admin2"]

            if admin1 == "":
                lowPrecisionLoc = None
            else:
                lowPrecisionLoc = admin1 + ", " + probeAddress["cc"]

            if admin2 == "":
                highPrecisionLoc = None
            else:
                highPrecisionLoc = admin2 + ", " + lowPrecisionLoc

            adminCoordinates = {"lat":probeAddress["lat"],"lon":probeAddress["lon"]}

        record["admin1"] = lowPrecisionLoc
        record["admin2"] = highPrecisionLoc
        record["adminCoordinates"] = adminCoordinates

        return record

    def flagNoisyProbe(self, record):
        # check if it is a noisy probe

        num_disco = len(self.disco_count[record['id']])

        if num_disco > 16:
            record['status']['name'] = 'Noisy' 
            record['status']['num_disco'] = num_disco

        return record

    def start(self):
        link = "https://atlas.ripe.net/api/v2/probes/archive?day="+self.date

        data = requests.get(link).json()
        """
        # Use this for loading probe information from a local file
        with open("../data/probeArchives/2016-12-12.json") as myFile:
            data = json.loads(myFile.read())
    
        """

        filename = data["source_filename"]
        timestamp = data["snapshot_datetime"]
        data = data["results"]

        count = 0

        for record in data:
            #Push individual record to Kafka
            record["source_filename"] = filename
            record["snapshot_datetime"] = timestamp
            record = self.augmentWithLocation(record)
            record = self.flagNoisyProbe(record)

            currentId = record["id"]

            timestampInMS = timestamp * 1000

            count += 1
            if count % 1000 == 0:
                print(count," records pushed")

            self.producer.send(self.topicName,key=bytes(str(currentId), "utf-8"),value=record,timestamp_ms=timestampInMS) 


#EXAMPLE
pdp = ProbeDataProducer()
pdp.adjustConfig()
pdp.start()


