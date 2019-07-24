"""
Reports Outage Events in Realtime
"""

from eventConsumer import EventConsumer
from streamSplitter import StreamSplitter
from burstDetector import BurstDetector
from kafka import KafkaProducer
import msgpack

import threading
from probeTracker import ProbeTracker

class Disco():
    def __init__(self,threshold,timeWindow,probeData):
        self.threshold = threshold
        self.timeWindow = timeWindow

        self.probeData = probeData
        self.eventData = []

        self.numTotalProbes = {}
        self.initNumProbes()

        self.disconnectedProbes = {}

        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', acks=0,
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            batch_size=65536,linger_ms=4000,compression_type='gzip')

        self.topicName = "ihr_disco_burst"

    def initNumProbes(self):
        self.numTotalProbes["ASN"] = {}
        self.numTotalProbes["COUNTRY"] = {}
        self.numTotalProbes["ADMIN1"] = {}
        self.numTotalProbes["ADMIN2"] = {}

        for probeId, probeDatum in self.probeData.items():
            probeASNv4 = probeDatum["asn_v4"]
            probeASNv6 = probeDatum["asn_v6"]

            probeCountry = probeDatum["country_code"]
            probeAdmin1 = probeDatum["admin1"]
            probeAdmin2 = probeDatum["admin2"]

            if probeASNv4 not in self.numTotalProbes["ASN"].keys():
                self.numTotalProbes["ASN"][probeASNv4] = 1
            else:
                self.numTotalProbes["ASN"][probeASNv4] += 1

            if probeASNv4 != probeASNv6:
                if probeASNv6 not in self.numTotalProbes["ASN"].keys():
                    self.numTotalProbes["ASN"][probeASNv6] = 1
                else:
                    self.numTotalProbes["ASN"][probeASNv6] += 1

            if probeCountry not in self.numTotalProbes["COUNTRY"].keys():
                self.numTotalProbes["COUNTRY"][probeCountry] = 1
            else:
                self.numTotalProbes["COUNTRY"][probeCountry] += 1

            if probeAdmin1 not in self.numTotalProbes["ADMIN1"].keys():
                self.numTotalProbes["ADMIN1"][probeAdmin1] = 1
            else:
                self.numTotalProbes["ADMIN1"][probeAdmin1] += 1

            if probeAdmin2 not in self.numTotalProbes["ADMIN2"].keys():
                self.numTotalProbes["ADMIN2"][probeAdmin2] = 1
            else:
                self.numTotalProbes["ADMIN2"][probeAdmin2] += 1 

    def trackDisconnectedProbes(self,streamType,streamName,startTime,disconnectedProbes):
        tracker = ProbeTracker(streamType,streamName,startTime,disconnectedProbes)
        tracker.start()
        del tracker

    def eventDataProcessor(self,data):
        #get event probe
        eventProbeId = data["prb_id"]
        eventType = data["event"]

        #see if it is relevant
        if (eventType == "disconnect") and (eventProbeId in self.probeData.keys()):
            self.eventData.append(data)

    def addDisconnectedProbe(self,streamName,probeId,timeStamp):
        if streamName not in self.disconnectedProbes.keys():
            self.disconnectedProbes[streamName] = {probeId:timeStamp}
        else:
            self.disconnectedProbes[streamName][probeId] = timeStamp

    def cleanDisconnectedProbes(self,disconnectedProbes,outageTime,window):  #Gives the probes disconnected within the time window of burst starting time
        startThreshold = outageTime - (window/2)
        endThreshold = outageTime + (window/2)

        cleanedDisconnectedProbes = {}

        for probeId, timeStamp in disconnectedProbes.items():
            if (timeStamp >= startThreshold) and (timeStamp <= endThreshold):
                cleanedDisconnectedProbes[probeId] = timeStamp

        return cleanedDisconnectedProbes

    def pushEventsToKafka(self,bursts):
        for streamType, burstByStream in bursts.items():
            for streamName, burstsArr in burstByStream.items():
                burstEvent = burstsArr[0] #for now just pick first event
                level = burstEvent[0]
                startTime = burstEvent[1]

                disconnectedProbes = self.disconnectedProbes[streamName]
                disconnectedProbes = self.cleanDisconnectedProbes(disconnectedProbes,startTime,self.timeWindow)
                totalProbes = self.numTotalProbes[streamType][streamName]

                event = {}
                event["streamtype"] = streamType
                event["streamname"] = streamName
                event["starttime"] = startTime
                event["level"] = level
                event["probelist"] = disconnectedProbes
                event["totalprobes"] = totalProbes

                self.producer.send(self.topicName,event,timestamp_ms=int(startTime*1000))
                threading.Thread(target=self.trackDisconnectedProbes, args=(streamType,streamName,startTime,disconnectedProbes)).start()

    def updateDisconnectedProbes(self,centralTimeStamp,eventData):
        startThreshold = centralTimeStamp - (2*self.timeWindow) 

        #clear all data older than threshold
        idsToRemove = []

        for streamName, probesInStream in self.disconnectedProbes.items():
            for probeId, timeStamp in probesInStream.items():
                if timeStamp < startThreshold:
                    idsToRemove.append(probeId)

        streamNamesToRemove = []

        for streamName, probesInStream in self.disconnectedProbes.items():
            for probeId in idsToRemove:
                if probeId in probesInStream.keys():
                    del probesInStream[probeId]

            if len(probesInStream) == 0:
                streamNamesToRemove.append(streamName)

        for streamName in streamNamesToRemove:
            del self.disconnectedProbes[streamName]


        #store new disconnect events
        for event in eventData:
            probeId = event["prb_id"]
            timeStamp = event["timestamp"]

            probeDatum = self.probeData[probeId]

            probeASNv4 = probeDatum["asn_v4"]
            if probeASNv4 is not None:
                self.addDisconnectedProbe(probeASNv4,probeId,timeStamp)

            probeASNv6 = probeDatum["asn_v6"]
            if (probeASNv6 is not None) and (probeASNv4 != probeASNv6):
                self.addDisconnectedProbe(probeASNv6,probeId,timeStamp)

            probeCountry = probeDatum["country_code"]
            if probeCountry is not None:
                self.addDisconnectedProbe(probeCountry,probeId,timeStamp)

            probeAdmin1 = probeDatum["admin1"]
            if probeAdmin1 is not None:
                self.addDisconnectedProbe(probeAdmin1,probeId,timeStamp)

            probeAdmin2 = probeDatum["admin2"]
            if probeAdmin2 is not None:
                self.addDisconnectedProbe(probeAdmin2,probeId,timeStamp)

    def start(self):
        #timeStamp = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())

        #try with test case
        timeStamp = 1553385638
        while True:
            eventReader = EventConsumer(timeStamp,self.timeWindow)
            eventReader.attach(self)
            eventReader.start()

            self.updateDisconnectedProbes(timeStamp,self.eventData)

            streamSplitter = StreamSplitter(self.probeData)
            streams = streamSplitter.getStreams(self.eventData)

            burstDetector = BurstDetector(streams,self.probeData,timeRange=self.timeWindow)
            bursts = burstDetector.detect(threshold=self.threshold)

            self.pushEventsToKafka(bursts)

            self.eventData = []
            timeStamp += self.timeWindow

"""
#EXAMPLE 
from disco import Disco 
Disco(threshold=10,timeWindow=3600*24,probeData=self.probeData).start()
"""
