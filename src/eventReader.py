from kafka import KafkaConsumer
import json
import time

from datetime import datetime

import logging 

from kafka.structs import TopicPartition, OffsetAndTimestamp

class EventReader():
    def __init__(self,startTS,windowInMS):
        self.topicName = "atlasLiveData"
        self.startTS = startTS

        self.consumer = KafkaConsumer(auto_offset_reset="earliest",bootstrap_servers=['localhost:9092'],consumer_timeout_ms=1000,value_deserializer=lambda v: msgpack.unpackb(v, raw=False))
        self.topicPartition = TopicPartition(self.topicName,0)

        self.windowSize = windowInMS #milliseconds

        self.observers = []

    def attach(self,observer):
        if observer not in self.observers:
            self.observers.append(observer)

    def performUpdate(self,data):
        for observer in self.observers:
            observer.updateFunc()

    def start(self):
        timestampToSeek = self.startTS * 1000
        timestampToBreakAt = timestampToSeek + self.windowSize

        print("Time Start: ",timestampToSeek,", Time End: ",timestampToBreakAt)

        offsets = self.consumer.offsets_for_times({self.topicPartition:timestampToSeek})

        while offsets[self.topicPartition] is None:
            #recheck after 20 seconds
            time.sleep(20) 

            currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
            if currentTS > timestampToBreakAt:
                return

            offsets = self.consumer.offsets_for_times({self.topicPartition:timestampToSeek})

        print(offsets)

        theOffset = offsets[self.topicPartition].offset

        self.consumer.assign([self.topicPartition])

        self.consumer.seek(self.topicPartition,theOffset)

        for message in self.consumer:
            messageTimestamp = message.timestamp

            if messageTimestamp > timestampToBreakAt:
                break

            msgAsDict = message.value

            print(msgAsDict)

            #self.performUpdate(msgAsDict)

#EXAMPLE
"""
currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
eventReader = EventReader(currentTS,600*1000)
eventReader.start()"""