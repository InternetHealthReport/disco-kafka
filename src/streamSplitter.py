from geopy.geocoders import Nominatim

class StreamSplitter():
    def __init__(self,probeData,splitByASN=True,splitByCity=True,splitByCountry=True):
        self.probeIDToCountry = {}
        self.probeIDToCity = {}
        self.initProbeLocations(probeData)

        self.splitByASN = splitByASN
        self.splitByCity = splitByCity
        self.splitByCountry = splitByCountry

        self.asnStreams = {}
        self.cityStreams = {}
        self.countryStreams = {}

    def initProbeLocations(self,probeData):
        geolocator = Nominatim(user_agent="specify_your_app_name_here")

        for probeId, probeDatum in probeData.items():
            country = probeDatum["country_code"]
            self.probeIDToCountry[probeId] = country

            try:
                probeLat = probeDatum["geometry"]["coordinates"][0]
                probeLong = probeDatum["geometry"]["coordinates"][1]

                query = str(probeLat)+"00, "+str(probeLong)+"00"
                #probeCity = geolocator.reverse(query)
                probeCity = geolocator.reverse([probeLat,probeLong])
                self.probeIDToCity[probeId] = probeCity
            except Exception as e:
                print(e)

    def addEvent(self,event):
        eventProbeID = event["prb_id"]

        if self.splitByASN:
            eventASN = event["asn"]

            if eventASN not in self.asnStreams.keys():
                asnStreams[eventASN] = [event]
            else:
                asnStreams[eventASN].append(event)

        
        if self.splitByCity:
            try:
                eventCity = self.probeIDToCity[eventProbeID]
                if eventCity not in self.cityStreams.keys():
                    self.cityStreams[eventCity] = [event]
                else:
                    self.cityStreams[eventCity].append(event)
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

        return {"ASN":self.asnStreams,"CITY":self.cityStreams,"COUNTRY":self.countryStreams}

