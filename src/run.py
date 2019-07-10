from eventConsumer import EventConsumer
from probeDataConsumer import ProbeDataConsumer
from streamSplitter import StreamSplitter
from burstDetector import BurstDetector
from datetime import datetime

class Tester():
    def __init__(self,eventType):
        self.eventType = eventType

        self.probeData = {}
        self.eventData = []

    def probeDataProcessor(self,data):
        probeId = data["id"]
        self.probeData[probeId] = data


    def eventDataProcessor(self,data):
        #get event probe
        eventProbeId = data["prb_id"]

        #see if it is relevant
        if eventProbeId in self.probeData.keys():
            self.eventData.append(data)

    def run(self):
        #Populate probe id's
        probeCon = ProbeDataConsumer()
        probeCon.attach(self)
        probeCon.start()

        #start reading events
        while True:
            currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
            eventReader = EventConsumer(currentTS,60*1000)
            eventReader.attach(self)
            eventReader.start()

            print("Number of events: ",len(self.eventData))

            streamSplitter = StreamSplitter(self.probeData)
            streams = streamSplitter.getStreams(self.eventData)

            burstDetector = BurstDetector(streams,self.probeData,timeRange=8640000)
            bursts = burstDetector.detect()

            print(bursts)

            self.eventData = []
            del eventReader, streamSplitter, burstDetector
        
        print("Done!")


tester = Tester("disconnect")
tester.run()












"""
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="specify_your_app_name_here")
probeCity = geolocator.reverse("6.1285, 45.9005")
print(probeCity.raw)
"""

"""
from googlegeocoder import GoogleGeocoder
geocoder = GoogleGeocoder("AIzaSyAAKBQMoxN-O0Zv_oO7N7y89b2SQESmkLo")
reverse = geocoder.get((33.9395164, -118.2414404))
print(reverse)
"""
