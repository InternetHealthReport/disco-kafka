import sys
import argparse
from probeDataConsumer import ProbeDataConsumer
from datetime import datetime
from disco import Disco 

class Runner():
    def __init__(self,threshold,startTime,endTime,timeWindow,countryFilters,asnFilters,proximityFilters):
        self.probeData = {}

        self.threshold = threshold
        self.startTime = startTime
        self.endTime = endTime
        self.timeWindow = timeWindow
        self.countryFilters = countryFilters
        self.asnFilters = asnFilters
        self.proximityFilters = proximityFilters

    def probeDataProcessor(self,data):
        probeId = data["id"]
        self.probeData[probeId] = data

    def run(self):
        #Populate probe id's
        probeCon = ProbeDataConsumer(countryFilters=self.countryFilters,asnFilters=self.asnFilters,proximityFilters=self.proximityFilters)
        probeCon.attach(self)
        probeCon.start()

        print(len(self.probeData))

        Disco(threshold=self.threshold,startTime=self.startTime,endTime=self.endTime,timeWindow=self.timeWindow,probeData=self.probeData).start()



if __name__ == '__main__':

    text = "Internet outage detection with RIPE Atlas disconnections"

    parser = argparse.ArgumentParser(description = text)  
    parser.add_argument("--threshold","-t",help="Choose burst threshold level")
    parser.add_argument("--startTime","-s",help="Choose start time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--endTime","-e",help="Choose end time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--timeWindow","-w",help="Choose a time window in seconds to read disco events (Example: 3600)")
    parser.add_argument("--countryFilters","-c",help='Give a list of countries to focus on (Example: ["AT","PK"])')
    parser.add_argument("--asnFilters","-a",help='Give a list of ASNs to focus on (Example: [57169,373])')
    parser.add_argument("--proximityFilters","-p",help='Give a list of lat,long pairs (Example: [[16.4375,47.3695],[15.4375,47.3695]])')

    args = parser.parse_args() 

    if args.threshold:
        threshold = int(args.threshold)
    else:
        sys.exit("Error: Threshold not specified; for a list of args, type python3 run.py -h")

    if args.startTime:
        startTime = datetime.strptime(args.startTime+"UTC", "%Y-%m-%dT%H:%M:%S%Z")
        startTime = int((startTime - datetime(1970, 1, 1)).total_seconds())
    else:
        startTime = None

    if args.endTime:
        endTime = datetime.strptime(args.endTime+"UTC", "%Y-%m-%dT%H:%M:%S%Z")
        endTime = int((endTime - datetime(1970, 1, 1)).total_seconds())
    else:
        endTime = None

    if args.timeWindow:
        timeWindow = int(args.timeWindow)
    else:
        timeWindow = 3600*24

    if args.countryFilters:
        countryFilters = list(args.countryFilters)
    else:
        countryFilters = []

    if args.asnFilters:
        asnFilters = eval(args.asnFilters)
    else:
        asnFilters = []

    if args.proximityFilters:
        proximityFilters = eval(args.proximityFilters)
    else:
        proximityFilters = []


    Runner(threshold,startTime,endTime,timeWindow,countryFilters,asnFilters,proximityFilters).run()

