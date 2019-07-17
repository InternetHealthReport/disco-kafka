"""
Reads latest probe data from Kafka
"""

from kafka import KafkaConsumer
from utils import haversine
import msgpack

class ProbeDataConsumer():
    def __init__(self,asnFilters=[],countryFilters=[],proximityFilters=[]):
        self.topicName = "ihr_atlas_probe_archive"

        self.consumer = KafkaConsumer(self.topicName,auto_offset_reset="earliest",bootstrap_servers=['localhost:9092'],consumer_timeout_ms=1000,value_deserializer=lambda v: msgpack.unpackb(v, raw=False))

        self.asnFilters = asnFilters
        self.countryFilters = countryFilters
        self.proximityFilters = proximityFilters

        self.proximityThreshold = 50

        self.observers = []

    def isRelevant(self,record):
        """
        Checks whether record passes the specified filters
        """
        
        if record["status"]["name"] == "Abandoned":     #Abandoned probes should be ignored
            return False

        probeASNv4 = record["asn_v4"]
        probeASNv6 = record["asn_v6"]
        
        asnCheckPassed = False

        if self.asnFilters == []:
            asnCheckPassed = True
        else:
            for asn in self.asnFilters:
                if (probeASNv4 == asn) or (probeASNv6 == asn):
                    asnCheckPassed = True
                    break



        probeCountry = record["country_code"]

        countryCheckPassed = False

        if self.countryFilters == []:
            countryCheckPassed = True
        else:
            for country in self.countryFilters:
                if probeCountry == country:
                    countryCheckPassed = True
                    break



        probeLocation = record["geometry"]["coordinates"]

        probeLat = probeLocation[0]
        probeLongt = probeLocation[1]

        proximityCheckPassed = False

        if self.proximityFilters == []:
            proximityCheckPassed = True
        elif (probeLat is None) or (probeLongt is None):
            #A lot of cases without coordinates. Currently discarding these probes
            proximityCheckPassed = False
        else:
            for coordinates in self.proximityFilters:
                lat = coordinates[0]
                longt = coordinates[1]

                distance = haversine(probeLat,probeLongt,lat,longt)

                if distance <= self.proximityThreshold:
                    proximityCheckPassed = True
                    break


        if asnCheckPassed and countryCheckPassed and proximityCheckPassed:
            return True
        else:
            return False

    def attach(self,observer):
        if observer not in self.observers:
            self.observers.append(observer)

    def notifyObservers(self,data):
        for observer in self.observers:
            observer.probeDataProcessor(data)

    def start(self):
        for message in self.consumer:
            record = message.value

            if self.isRelevant(record):
                self.notifyObservers(record)

"""
#EXAMPLE

probeCon = ProbeDataConsumer(asnFilters=[57169],countryFilters=["AT"],proximityFilters=[[16.4375,47.3695],[15.4375,47.3695]])
probeCon.attach(object) #Attach object that defines probeDataProcessor function
probeCon.start()"""
