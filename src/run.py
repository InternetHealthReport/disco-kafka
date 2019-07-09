from eventConsumer import EventConsumer
from probeDataConsumer import ProbeDataConsumer
from streamSplitter import StreamSplitter
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

            streamSplitter = StreamSplitter(self.probeData)
            streams = streamSplitter.getStreams(self.eventData)

            print(streams)

            self.eventData = []
            del eventReader, streamSplitter
        
        print("Done!")

tester = Tester("disconnect")
tester.run()