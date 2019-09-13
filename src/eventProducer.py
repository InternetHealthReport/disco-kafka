"""
Pushes Atlas Live Events to Kafka indefinitely
"""

import sys
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from datetime import datetime
import time
import json
from ripe.atlas.cousteau import AtlasResultsRequest
import msgpack
import argparse
import arrow

class EventProducer():
    def __init__(self, topicName):
        admin_client = KafkaAdminClient(
               bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'], 
                client_id='disco_eventproducer_admin')

        try:
            topic_list = [NewTopic(name=topicName, num_partitions=1, replication_factor=2, topic_configs={'retention.ms':30758400000})]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except Exception as e:
            logging.warning(str(e))
            pass
        finally:
            admin_client.close()

        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            compression_type='snappy')

        self.topicName = topicName

    def startLive(self, endTime=None):
        WINDOW = 5*60
        currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
        while endTime is None or endTime>currentTS:
            try:
                kwargs = {
                    "msm_id": 7000,
                    "start": datetime.utcfromtimestamp(currentTS-2*WINDOW),
                    "stop": datetime.utcfromtimestamp((currentTS-WINDOW) -1),
                }

                is_success, results = AtlasResultsRequest(**kwargs).create()
                if is_success:
                    for ent in results:
                        timestamp = ent["timestamp"]
                        timestamp = timestamp*1000      #convert to milliseconds
                        self.producer.send(self.topicName,ent,timestamp_ms=timestamp)
                else:
                    logging.error("Fetch Failed! {}".format(kwargs))

                time.sleep(WINDOW)
                currentTS += WINDOW 
            except Exception as e:
                logging.error("Error: ",e)

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
        else:
            logging.error("Fetch Failed! {}".format(kwargs))

    def startBigPeriod(self,startTS,endTS):
        timeStamp = startTS
        window = 3600*24*3

        while timeStamp < endTS:
            self.startPeriod(timeStamp,timeStamp+window)
            timeStamp += window


if __name__ == '__main__':
    # Command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument( "-t","--topic", type=str,
            help="Kafka topic where data is pushed (e.g. 'ihr_atlas_probe_discolog')", default='default_atlas_probe_discolog')
    parser.add_argument( "-s","--startTime", type=str, help="Start time for fetching data")
    parser.add_argument( "-e","--endTime", type=str, help="End time for fetching data")
    args = parser.parse_args()

    # Logging 
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, filename='disco-eventproducer.log' , 
            level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)
    logging.info("Arguments: %s" % args)

    start_time = time.time()

    ep = EventProducer(args.topic)

    if args.startTime and args.endTime:
        start = arrow.get(args.startTime)
        end = arrow.get(args.endTime)
        ep.startPeriod(start.timestamp, end.timestamp)
    else:
        # Get live data
        ep.startLive()

    elapsed_time = time.time() - start_time
    logging.warning(elapsed_time)
