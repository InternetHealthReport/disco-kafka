"""
Given a list of disconnected probes, reports when 50% of them are reconnected
"""
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import msgpack
import numpy as np
import logging

class ProbeTracker():
    def __init__(self,streamType,streamName,startTime,disconnectedProbes, level,
            topicIn, topicOut):
        self.streamType = streamType
        self.streamName = streamName
        self.startTime = startTime
        self.level = level
        self.topicIn = topicIn
        self.disconnectedProbes = disconnectedProbes
        self.nbProbesThreshold = len(disconnectedProbes)/2
        self.reconnectedProbes = {}
        self.topicName = topicOut

        admin_client = KafkaAdminClient(
                bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'], 
                client_id='disco_disco_admin')

        try:
            topic_list = [NewTopic(name=self.topicName, num_partitions=1, replication_factor=2, topic_configs={'retention.ms':30758400000})]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except Exception as e:
            pass
        finally:
            admin_client.close()

        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', 
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            compression_type='snappy')

        self.consumer = KafkaConsumer(
                enable_auto_commit=False,
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda v: msgpack.unpackb(v, raw=False)
                )
        self.consumer.subscribe(topicIn)

        # print("Tracker Init! {} {} {} {}".format(streamType, streamName, startTime, disconnectedProbes))

    def getTimeForLastReconnect(self):
        return np.median(list(self.reconnectedProbes.values()))

    def pushEventToKafka(self,endTime):
        event = {}
        event["streamtype"] = self.streamType
        event["streamname"] = self.streamName
        event["starttime"] = self.startTime
        event["endtime"] = endTime
        event["duration"] = endTime - self.startTime
        event["level"] = self.level
        event["reconnectedprobes"] = self.reconnectedProbes

        self.producer.send(self.topicName,event,timestamp_ms=int(endTime*1000))
        self.producer.flush()

    def eventDataProcessor(self,data):
        #get event probe
        eventProbeId = data["prb_id"]
        eventType = data["event"]
        eventTime = data["timestamp"]

        if eventType == "connect":
            if eventProbeId in self.disconnectedProbes.keys():
                disconnectTime = self.disconnectedProbes[eventProbeId]

                if eventTime > disconnectTime:
                    self.reconnectedProbes[eventProbeId] = eventTime


    def start(self):    #returns when done hence notifying calling thread to quit
        try:
            timeStampToSeek = self.startTime*1000

            tp = TopicPartition(self.topicIn, 0)
            offsets = self.consumer.offsets_for_times({tp: timeStampToSeek})
            for partition, toffset in offsets.items():
                self.consumer.seek(partition, toffset.offset)

            stop = False
            while not stop:
                msg_poll = self.consumer.poll()
                for tp, messages in msg_poll.items():
                    for message in messages:
                        self.eventDataProcessor(message.value)

                    if len(self.reconnectedProbes) > self.nbProbesThreshold:
                        self.pushEventToKafka(self.getTimeForLastReconnect())
                        stop = True
                        break

            self.producer.close()
            self.consumer.close()

        except Exception as e:
            print(e)







