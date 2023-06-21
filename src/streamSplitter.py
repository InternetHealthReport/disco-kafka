"""
Splits events into separate streams based on ASN, Country or Finer Locality
"""

import time

class StreamSplitter():
    def __init__(self,probeData,splitByASN=True,splitByCountry=True,splitByAdmin1=True,splitByAdmin2=True):
        self.probeIDToCountry = {}
        self.probeIDToAdmin1 = {}
        self.probeIDToAdmin2 = {}
        self.initProbeLocations(probeData)

        self.splitByASN = splitByASN
        self.splitByCountry = splitByCountry
        self.splitByAdmin1 = splitByAdmin1
        self.splitByAdmin2 = splitByAdmin2

        self.asnStreams = {}
        self.countryStreams = {}
        self.admin1Streams = {}
        self.admin2Streams = {}

    def initProbeLocations(self,probeData):
        #geolocator = Nominatim(user_agent="specify_your_app_name_here")

        for probeId, probeDatum in probeData.items():
            country = probeDatum["country_code"]
            self.probeIDToCountry[probeId] = country

            admin1 = probeDatum["admin1"]
            self.probeIDToAdmin1[probeId] = admin1

            admin2 = probeDatum["admin2"]
            self.probeIDToAdmin2[probeId] = admin2

    def addEvent(self,event):
        eventProbeID = event["prb_id"]

        if self.splitByASN:
            eventASN = event["asn"]

            if eventASN not in self.asnStreams.keys():
                self.asnStreams[eventASN] = [event]
            else:
                self.asnStreams[eventASN].append(event)

        if self.splitByCountry:
            try:
                eventCountry = self.probeIDToCountry[eventProbeID]
                if eventCountry not in self.countryStreams.keys():
                    self.countryStreams[eventCountry] = [event]
                else:
                    self.countryStreams[eventCountry].append(event)
            except Exception as e:
                print("Error while splitting country: ",e)

        if self.splitByAdmin1:
            try:
                eventAdmin1 = self.probeIDToAdmin1[eventProbeID]
                if eventAdmin1 and (eventAdmin1 not in self.admin1Streams.keys()):
                    self.admin1Streams[eventAdmin1] = [event]
                else:
                    self.admin1Streams[eventAdmin1].append(event)
            except Exception as e:
                if e != "None":
                    #print("Error while splitting admin1: ",e)
                    pass

        if self.splitByAdmin2:
            try:
                eventAdmin2 = self.probeIDToAdmin2[eventProbeID]
                if eventAdmin2 and (eventAdmin2 not in self.admin2Streams.keys()):
                    self.admin2Streams[eventAdmin2] = [event]
                else:
                    self.admin2Streams[eventAdmin2].append(event)
            except Exception as e:
                pass
        

    def getStreams(self,events):
        self.asnStreams = {}
        self.countryStreams = {}
        self.admin1Streams = {}
        self.admin2Streams = {}

        for event_probe in events.values():
            for event in event_probe:
                self.addEvent(event)

        return {"ASN":self.asnStreams,"COUNTRY":self.countryStreams,"ADMIN1":self.admin1Streams,"ADMIN2":self.admin2Streams}

