"""
Performs kleinberg on event streams to report burst levels
"""

import numpy as np
from pybursts import pybursts
import logging

class BurstDetector():
    def __init__(self,streams,probeData,timeRange):
        self.asnStreams = streams["ASN"]
        self.countryStreams = streams["COUNTRY"]
        self.admin1Streams = streams["ADMIN1"]
        self.admin2Streams = streams["ADMIN2"]
        #TODO: add stateStreams

        self.probeData = probeData
        self.numTotalProbes = {}
        self.initNumProbes()

        self.timeRange = timeRange

        self.minProbes = 5
        self.minSignalLength = 5

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

    def kleinberg(self,timeSeries,numOfProbes):
        timeSeries = np.array(timeSeries)
    
        bursts = pybursts.kleinberg(timeSeries,s=2,gamma=0.5,T=self.timeRange,n=numOfProbes)
        # if len(bursts) > 10:
            # print("--------------------")
            # print(timeSeries)
            # print(bursts)
        
        return bursts

    def getTimeSeries(self,stream):
        timeStampCounts = {}
        for event in stream:
            eventTimeStamp = float(event['timestamp'])

            if eventTimeStamp not in timeStampCounts.keys():
                timeStampCounts[eventTimeStamp] = 1
            else:
                timeStampCounts[eventTimeStamp] += 1

        timeSeries = []
        for timeStamp, count in timeStampCounts.items():
            for step in range(0,count):
                timeSeries.append((timeStamp)+(step/count))

        timeSeries.sort()

        return timeSeries

    def cleanBurstData(self,bursts,threshold):
        cleaned = []
        for datum in bursts:
            score = datum[0]

            if score >= threshold:
                cleaned.append(datum)

        return cleaned


    def detect(self,threshold=0):
        #ASN Streams
        burstsByASN = {}

        for asn, stream in self.asnStreams.items():
            numTotalProbes = self.numTotalProbes["ASN"].get(asn, 0)

            if numTotalProbes < self.minProbes:
                continue

            timeSeries = self.getTimeSeries(stream)
            if len(timeSeries) < self.minSignalLength:
                continue

            bursts = self.kleinberg(timeSeries,numTotalProbes)
            bursts = self.cleanBurstData(bursts,threshold)

            if len(bursts) > 0:
                burstsByASN[asn] = bursts

        #Country Streams
        burstsByCountry = {}

        for country, stream in self.countryStreams.items():
            numTotalProbes = self.numTotalProbes["COUNTRY"][country]

            if numTotalProbes < self.minProbes:
                continue

            timeSeries = self.getTimeSeries(stream)
            bursts = self.kleinberg(timeSeries,numTotalProbes)
            bursts = self.cleanBurstData(bursts,threshold)

            if len(bursts) > 0:
                burstsByCountry[country] = bursts

        #Admin1 Streams
        burstsByAdmin1 = {}

        for admin1, stream in self.admin1Streams.items():
            numTotalProbes = self.numTotalProbes["ADMIN1"][admin1]

            if numTotalProbes < self.minProbes:
                continue

            timeSeries = self.getTimeSeries(stream)
            bursts = self.kleinberg(timeSeries,numTotalProbes)
            bursts = self.cleanBurstData(bursts,threshold)

            if len(bursts) > 0:
                burstsByAdmin1[admin1] = bursts

        #Admin1 Streams
        burstsByAdmin2 = {}

        for admin2, stream in self.admin2Streams.items():
            numTotalProbes = self.numTotalProbes["ADMIN2"][admin2]

            if numTotalProbes < self.minProbes:
                continue

            timeSeries = self.getTimeSeries(stream)
            bursts = self.kleinberg(timeSeries,numTotalProbes)
            bursts = self.cleanBurstData(bursts,threshold)

            if len(bursts) > 0:
                burstsByAdmin2[admin2] = bursts

        return {"ASN":burstsByASN, "COUNTRY":burstsByCountry, "ADMIN1":burstsByAdmin1, "ADMIN2":burstsByAdmin2}



        
