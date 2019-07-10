from geopy.geocoders import Nominatim
import time

class StreamSplitter():
    def __init__(self,probeData,splitByASN=True,splitByState=True,splitByCountry=True):
        self.probeIDToCountry = {}
        self.probeIDToState = {}
        self.initProbeLocations(probeData)

        self.splitByASN = splitByASN
        self.splitByState = splitByState
        self.splitByCountry = splitByCountry

        self.asnStreams = {}
        self.stateStreams = {}
        self.countryStreams = {}

    def initProbeLocations(self,probeData):
        #geolocator = Nominatim(user_agent="specify_your_app_name_here")

        for probeId, probeDatum in probeData.items():
            country = probeDatum["country_code"]
            self.probeIDToCountry[probeId] = country

            """ #Move state finding to probeDataProducer
            try:
                probeLat = probeDatum["geometry"]["coordinates"][0]
                probeLong = probeDatum["geometry"]["coordinates"][1]

                #query = str(probeLat)+"00, "+str(probeLong)+"00"
                #probeCity = geolocator.reverse(query)
                #query = (probeLat,probeLong)
                #print("Query: ",query)
                probeAddress = geolocator.reverse("{}, {}".format(probeLat,probeLong)).raw["address"]]
                probeState = probeAddress["state"]

                self.probeIDToState[probeId] = probeState
            except Exception as e:
                print("Error in reverse func: ",e,)"""
        

    def addEvent(self,event):
        eventProbeID = event["prb_id"]

        if self.splitByASN:
            eventASN = event["asn"]

            if eventASN not in self.asnStreams.keys():
                self.asnStreams[eventASN] = [event]
            else:
                self.asnStreams[eventASN].append(event)

        
        if self.splitByState:
            try:
                eventState = self.probeIDToState[eventProbeID]
                if eventState not in self.stateStreams.keys():
                    self.stateStreams[eventState] = [event]
                else:
                    self.stateStreams[eventState].append(event)
            except Exception as e:
                print(e)

        if self.splitByCountry:
            try:
                eventCountry = self.probeIDToCountry[eventProbeID]
                if eventCountry not in self.countryStreams.keys():
                    self.countryStreams[eventCountry] = [event]
                else:
                    self.countryStreams[eventCountry].append(event)
            except Exception as e:
                print(e)
        

    def getStreams(self,events):
        for event in events:
            self.addEvent(event)

        return {"ASN":self.asnStreams,"STATE":self.stateStreams,"COUNTRY":self.countryStreams}

