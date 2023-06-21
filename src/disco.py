"""
Reports Outage Events in Realtime
"""

from eventConsumer import EventConsumer
from streamSplitter import StreamSplitter
from burstDetector import BurstDetector
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import msgpack
from datetime import datetime
import logging
from collections import defaultdict
import numpy as np

import threading
from probeTracker import ProbeTracker

from concurrent.futures import ProcessPoolExecutor

def trackDisconnectedProbes(args):
    streamType = args[0]
    streamName = args[1]
    startTime = args[2]
    disconnectedProbes = args[3]
    burstLevel = args[4]
    topicIn = args[5]
    topicOut = args[6]
    tracker = ProbeTracker(streamType, streamName, startTime, disconnectedProbes, 
            burstLevel, topicIn, topicOut)
    tracker.start()
    del tracker


class Disco():
    def __init__(self,threshold,startTime,endTime,timeWindow,probeData, topicIn, topicOut, slideStep=3600):
        self.threshold = threshold

        self.startTime = startTime
        self.endTime = endTime
        self.timeWindow = timeWindow

        # Slide the window by this amount
        self.slideWindow = slideStep
        # Ignore disconnection events followed by a reconnect within
        # discoProbesWindow seconds
        # Also report only probes disconnected discoProbesWindow 
        # seconds before/after the burst starting time
        self.discoProbesWindow = 300

        self.probeData = probeData
        self.eventData = defaultdict(list)

        self.numTotalProbes = {}
        self.initNumProbes()

        self.disconnectedProbes = {}

        admin_client = KafkaAdminClient(
                bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'], 
                client_id='disco_disco_admin')

        try:
            topic_list = [NewTopic(name=topicOut, num_partitions=1, replication_factor=2, 
                topic_configs={'retention.ms':30758400000})]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except Exception as e:
            logging.warning(str(e))
            pass
        finally:
            admin_client.close()

        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', 
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            compression_type='snappy')

        self.topicIn = topicIn
        self.topicOut = topicOut 

        self.executor = ProcessPoolExecutor(max_workers=10)

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

    def eventDataProcessor(self,data):
        '''This method is called by eventConsumer every time there is a new
        message from Kafka.'''
        #get event probe
        eventProbeId = data["prb_id"]
        eventType = data["event"]

        #see if it is relevant
        if (eventType == "disconnect") and (eventProbeId in self.probeData.keys()):
            self.eventData[eventProbeId].append(data)
        if (eventType == "connect") and (eventProbeId in self.eventData):
            # Ignore disconnection quickly followed by a reconnect
            if (data['timestamp'] > self.eventData[eventProbeId][-1]['timestamp'] 
                    and data['timestamp'] - self.eventData[eventProbeId][-1]['timestamp'] < self.discoProbesWindow): 
                del self.eventData[eventProbeId][-1]
                if len(self.eventData[eventProbeId]) == 0:
                    del self.eventData[eventProbeId]


    def addDisconnectedProbe(self,streamName,probeId,timeStamp):
        if streamName not in self.disconnectedProbes:
            self.disconnectedProbes[streamName] = defaultdict(list)
        self.disconnectedProbes[streamName][probeId].append(timeStamp)

    def cleanDisconnectedProbes(self,disconnectedProbes, outageTime):  
        #Report only probes disconnected near the burst starting time
        startThreshold = outageTime - self.discoProbesWindow
        endThreshold = outageTime + self.discoProbesWindow

        cleanedDisconnectedProbes = {}

        for probeId, timeStamps in disconnectedProbes.items():
            for timeStamp in timeStamps:
                if (timeStamp >= startThreshold) and (timeStamp <= endThreshold):
                    cleanedDisconnectedProbes[probeId] = timeStamp

        return cleanedDisconnectedProbes


    def pushEventsToKafka(self,bursts):
        for streamType, burstByStream in bursts.items():
            for streamName, burstsArr in burstByStream.items():
                for burstEvent in burstsArr:
                    level = burstEvent[0]
                    startTime = burstEvent[1]

                    disconnectedProbes = self.disconnectedProbes.get(streamName, {})
                    disconnectedProbes = self.cleanDisconnectedProbes(disconnectedProbes,startTime)

                    if len(disconnectedProbes) == 0:
                        continue
                    
                    totalProbes = self.numTotalProbes[streamType][streamName]

                    event = {}
                    event["streamtype"] = streamType
                    event["streamname"] = streamName
                    event["starttime"] = startTime
                    event["level"] = level
                    event["probelist"] = disconnectedProbes
                    event["totalprobes"] = totalProbes

                    self.producer.send(self.topicOut,event,timestamp_ms=int(startTime*1000))
                    self.executor.submit(trackDisconnectedProbes,(streamType,streamName,startTime,disconnectedProbes, level, self.topicIn, self.topicOut+'_reconnect'))


    def updateDisconnectedProbes(self,centralTimeStamp):
        '''Remove old data (more than timeWindow-centralTimeStamp) '''

        startThreshold = centralTimeStamp - self.timeWindow 

        #clear all data older than threshold
        idsToRemove = []

        for streamName, probesInStream in self.disconnectedProbes.items():
            for probeId, timeStamps in probesInStream.items():
                if np.all(np.array(timeStamps) < startThreshold):
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
        for events in self.eventData.values():
            for event in events:
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

    def fallsWithin(self, timeStamp, periods):
        for period in periods:
            start = period[0]
            end = period[1]

            if (timeStamp >= start) and (timeStamp <= end):
                return True
        
        return False

    def getPeriod(self, timeStamp, periods):
        for period in periods:
            start = period[0]
            end = period[1]

            if (timeStamp >= start) and (timeStamp <= end):
                return period
        
        return (None,None)

    def cleanEvents(self,bursts,lastProcessedTimeStamp):
        toDelete = []
        burstLevelsAgainstPeriods = {}

        #Find redundant events
        for streamType, burstByStream in bursts.items():
            for streamName, burstsArr in burstByStream.items():
                i = 0
                indicesToDelete = []
                seenPeriods = []
                for burstEvent in burstsArr:
                    level = burstEvent[0]
                    startTime = burstEvent[1]
                    endTime = burstEvent[2]

                    if (startTime < lastProcessedTimeStamp) or self.fallsWithin(startTime,seenPeriods):
                        indicesToDelete.append(i)

                        if self.fallsWithin(startTime,seenPeriods):
                            period = self.getPeriod(startTime,seenPeriods)
                            burstLevelsAgainstPeriods[period].append(level)
                    else:
                        seenPeriods.append((startTime,endTime))
                        burstLevelsAgainstPeriods[(startTime,endTime)] = [level]


                    i += 1

                if len(indicesToDelete) != 0:
                    toDelete.append((streamType,streamName,indicesToDelete))
                

        #Delete redundant events
        for item in toDelete:
            streamType = item[0]
            streamName = item[1]
            indicesToDelete = item[2]
            indicesToDelete.reverse()

            for index in indicesToDelete:
                del bursts[streamType][streamName][index]

            if len(bursts[streamType][streamName]) == 0:
                del bursts[streamType][streamName]

        #Save only the highest burst level against each event
        for streamType, burstByStream in bursts.items():
            for streamName, burstsArr in burstByStream.items():
                for burstEvent in burstsArr:
                    burstPeriod = (burstEvent[1],burstEvent[2])

                    highestLevel = max(burstLevelsAgainstPeriods[burstPeriod])

                    burstEvent[0] = highestLevel

        return bursts

    def asDate(self,timestamp):
        return datetime.utcfromtimestamp(timestamp).strftime("%d-%b-%Y (%H:%M:%S)")


    def start(self):
        lastProcessedTimeStamp = 0

        if self.startTime is None:
            startTime = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
            # get data from the preceding window
            startTime = (startTime - self.timeWindow) + self.slideWindow
        else:
            startTime = self.startTime
            startTime = (startTime - self.timeWindow) + self.slideWindow

        burstDetector = BurstDetector(self.probeData, self.timeWindow)
        streamSplitter = StreamSplitter(self.probeData)

        while True:
            eventReader = EventConsumer(startTime,self.timeWindow, self.topicIn)
            eventReader.attach(self)
            eventReader.start()

            self.updateDisconnectedProbes(startTime)

            streams = streamSplitter.getStreams(self.eventData)

            burstDetector.initStreams(streams, self.timeWindow)
            bursts = burstDetector.detect(threshold=self.threshold)
            bursts = self.cleanEvents(bursts,lastProcessedTimeStamp)

            self.pushEventsToKafka(bursts)

            self.eventData = defaultdict(list)

            lastProcessedTimeStamp = startTime + self.timeWindow
            startTime += self.slideWindow

            logging.warning("Processed till {}".format(self.asDate(lastProcessedTimeStamp)))

            if self.endTime is not None:
                if lastProcessedTimeStamp >= self.endTime:
                    break

        logging.warning("Waiting for sub-processes")
        self.executor.shutdown(wait=True)

        logging.warning("End reached!")


"""
#EXAMPLE 
from disco import Disco 
Disco(threshold=7,timeWindow=3600*24,probeData=self.probeData).start()
"""
