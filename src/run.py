from probeDataProducer import ProbeDataProducer
from probeDataConsumer import ProbeDataConsumer

class Tester():
    def __init__(self):
        pass

    def hookFunc(self,data):
        print(data)

    def run(self):
        probeCon = ProbeDataConsumer(asnFilters=[],countryFilters=["AT","DE"],proximityFilters=[[16.4375,47.3695],[15.4375,47.3695]])
        probeCon.attach(self)
        probeCon.start()

Tester().run()
    