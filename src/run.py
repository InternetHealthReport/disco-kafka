from eventConsumer import EventConsumer
from probeDataConsumer import ProbeDataConsumer
from streamSplitter import StreamSplitter
from burstDetector import BurstDetector
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

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
        eventType = data["event"]

        #see if it is relevant
        if (eventType == self.eventType) and (eventProbeId in self.probeData.keys()):
            self.eventData.append(data)

    def run(self):
        start = 1553385600
        end = 1553644800

        #Populate probe id's
        probeCon = ProbeDataConsumer()
        probeCon.attach(self)
        probeCon.start()

        print("Num Probes: ",len(self.probeData))

        from disco import Disco 
        Disco(threshold=10,timeWindow=3600*24,probeData=self.probeData).start()


        """
        timeStamp = start
        windowToRead = 3600*24
        windowToSlide = 3600
        timesArray = []
        dataArray = []

        while timeStamp < end:
            eventReader = EventConsumer(timeStamp,windowToRead)
            eventReader.attach(self)
            eventReader.start()

            streamSplitter = StreamSplitter(self.probeData)
            streams = streamSplitter.getStreams(self.eventData)

            burstDetector = BurstDetector(streams,self.probeData,timeRange=windowToRead)
            bursts = burstDetector.detect()

            try:
                bursts = bursts["COUNTRY"]["VE"]

                for datum in bursts:
                    score = datum[0]
                    tsStart = datum[1]
                    tsEnd = datum[2]

                    theTimeStart = tsStart
                    theTimeEnd = tsEnd

                    dataArray.append(score)
                    timesArray.append(theTimeStart)

                    print(score,theTimeStart,theTimeEnd)
            except:
                pass

            print("No of events: ",len(self.eventData))
            print("-----")

            self.eventData = []
            del eventReader, streamSplitter, burstDetector

            timeStamp += windowToSlide

        maxScores = []
        scoreDict = {}
        for score, timeStart in zip(dataArray,timesArray):
            if timeStart not in scoreDict.keys():
                scoreDict[timeStart] = score
            else:
                oldScore = scoreDict[timeStart]

                if score > oldScore:
                    scoreDict[timeStart] = score

        import collections
        scoreDict = collections.OrderedDict(sorted(scoreDict.items()))

        print("Scores: ",scoreDict)

        maxScores = scoreDict.values()
        uniqueStartTimes = scoreDict.keys()
        asDates = [datetime.utcfromtimestamp(x).strftime("%d-%b-%Y (%H:%M:%S)") for x in uniqueStartTimes]
        
        import pandas as pd
        import matplotlib.dates as mdates

        d = ({"A":asDates,"B":list(maxScores)})
        df = pd.DataFrame(data=d)
        df['A'] = pd.to_datetime(df['A'], format="%d-%b-%Y (%H:%M:%S)")

        fig, ax = plt.subplots()
        ax.step(df["A"].values, df["B"].values)
        ax.set_xlim(df["A"].min(), df["A"].max())

        ax.xaxis.set_major_locator(mdates.MinuteLocator((0,30)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%b-%Y (%H:%M:%S)"))

        plt.xticks(rotation=75)
        plt.tight_layout()
        plt.show()
        
        print("Done!")"""


tester = Tester("disconnect")
tester.run()


"""
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType

admin = KafkaAdminClient()
admin.alter_configs([ConfigResource(ConfigResourceType.TOPIC,"ihr_atlas_probe_archive",{"retention.ms":1,"cleanup.policy":"compact"})])
desc = admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC,"ihr_atlas_probe_archive")])
print(desc)
"""

"""
import reverse_geocoder as rg

coordinates = (51.5214588,-0.1729636),(9.936033, 76.259952),(37.38605,-122.08385)
results = rg.search(coordinates) # default mode = 2
print(results)
"""

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
