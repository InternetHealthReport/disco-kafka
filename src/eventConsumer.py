"""
Reads Atlas Live Events from Kafka for a time window
"""

from kafka import KafkaConsumer
import json
import time

from datetime import datetime

import logging 
import msgpack

from kafka.structs import TopicPartition, OffsetAndTimestamp

class EventConsumer():
    def __init__(self,startTS,windowInSeconds):
        self.topicName = "ihr_atlas_live"
        self.startTS = startTS

        self.consumer = KafkaConsumer(auto_offset_reset="earliest",bootstrap_servers=['localhost:9092'],consumer_timeout_ms=1000,value_deserializer=lambda v: msgpack.unpackb(v, raw=False))
        self.topicPartition = TopicPartition(self.topicName,0)

        self.windowSize = windowInSeconds * 1000    #milliseconds

        self.observers = []

    def attach(self,observer):
        if observer not in self.observers:
            self.observers.append(observer)

    def notifyObservers(self,data):
        for observer in self.observers:
            observer.eventDataProcessor(data)

    def start(self):
        timestampToSeek = self.startTS * 1000
        timestampToBreakAt = timestampToSeek + self.windowSize

        #print("Time Start: ",timestampToSeek,", Time End: ",timestampToBreakAt)

        self.consumer.assign([self.topicPartition])

        offsets = self.consumer.offsets_for_times({self.topicPartition:timestampToSeek})

        while offsets[self.topicPartition] is None:
            #recheck after 10 seconds
            time.sleep(10) 

            currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())*1000
            if currentTS > timestampToBreakAt:
                return

            offsets = self.consumer.offsets_for_times({self.topicPartition:timestampToSeek})

        theOffset = offsets[self.topicPartition].offset

        self.consumer.seek(self.topicPartition,theOffset)
        
        for message in self.consumer:
            messageTimestamp = message.timestamp

            if messageTimestamp > timestampToBreakAt:
                break

            msgAsDict = message.value

            self.notifyObservers(msgAsDict)


"""
#EXAMPLE

currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
eventReader = EventConsumer(currentTS,600*1000)
eventReader.attach(object) #Attach object that defines eventDataProcessor function
eventReader.start()"""