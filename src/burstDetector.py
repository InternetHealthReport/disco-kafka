import numpy as np
import pybursts

class BurstDetector():
    def __init__(self,streams,probeData,timeRange):
        self.asnStreams = streams["ASN"]
        self.countryStreams = streams["COUNTRY"]
        #TODO: add stateStreams

        self.probeData = probeData
        self.numTotalProbes = {}
        self.initNumProbes()

        self.timeRange = timeRange

    def initNumProbes(self):
        self.numTotalProbes["ASN"] = {}
        self.numTotalProbes["COUNTRY"] = {}

        for probeId, probeDatum in probeData.items():
            probeASNv4 = probeDatum["asn_v4"]
            probeASNv6 = probeDatum["asn_v6"]

            probeCountry = probeDatum["country_code"]

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

    def kleinberg(self,timeSeries,numOfProbes):
        timeSeries = np.array(timeSeries)
    
        bursts = pybursts.kleinberg(timeStamps,s=2,gamma=0.5,T=self.timeRange,n=numOfProbes)
        
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


    def detect(self):
        #ASN Streams
        burstsByASN = {}

        for asn, stream in self.asnStreams.items():
            numTotalProbes = numTotalProbes["ASN"][asn]
            timeSeries = self.getTimeSeries(stream)
            bursts = self.kleinberg(timeSeries,numTotalProbes)

            burstsByASN[asn] = bursts

        #Country Streams
        burstsByCountry = {}

        for country, stream in self.countryStreams.items():
            numTotalProbes = numTotalProbes["COUNTRY"][country]
            timeSeries = self.getTimeSeries(stream)
            bursts = self.kleinberg(timeSeries,numTotalProbes)

            burstsByCountry[country] = bursts

        return {"ASN":burstsByASN, "COUNTRY":burstsByCountry}



        