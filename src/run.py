import sys
import argparse
import arrow
from probeDataConsumer import ProbeDataConsumer
from disco import Disco 
import logging

class Runner():
    def __init__(self,threshold,startTime,endTime,timeWindow,
            countryFilters,asnFilters,proximityFilters, 
            topicIn, topicOut):
        self.probeData = {}

        self.threshold = threshold
        self.startTime = startTime
        self.endTime = endTime
        self.timeWindow = timeWindow
        self.countryFilters = countryFilters
        self.asnFilters = asnFilters
        self.proximityFilters = proximityFilters

        self.topicIn = topicIn
        self.topicOut = topicOut

    def probeDataProcessor(self,data):
        probeId = data["id"]
        self.probeData[probeId] = data

    def run(self):
        #Populate probe id's
        probeCon = ProbeDataConsumer(
                self.asnFilters, self.countryFilters, self.proximityFilters, 
                self.startTime, self.endTime
                )
        probeCon.attach(self)
        probeCon.start()

        logging.warning('Loaded data for {} probes'.format(len(self.probeData)))

        Disco(
                threshold=self.threshold,startTime=self.startTime,endTime=self.endTime,timeWindow=self.timeWindow,
                probeData=self.probeData, topicIn=self.topicIn, topicOut=self.topicOut
            ).start()



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
    parser.add_argument("--topicAtlas","-i",help='Kafka topic for the input data (Atlas (dis)connection log)', default='default_atlas_discolog')
    parser.add_argument("--topicBurst","-o",help='Kafka topic for the output data (detected bursts of disconnections)', default='default_disco_burst')

    args = parser.parse_args() 

    if args.threshold:
        threshold = int(args.threshold)
    else:
        sys.exit("Error: Threshold not specified; for a list of args, type python3 run.py -h")

    if args.startTime:
        startTime = arrow.get(args.startTime).timestamp
    else:
        startTime = None

    if args.endTime:
        endTime = arrow.get(args.endTime).timestamp
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

    # Logging 
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, filename='ihr-kafka-disco-detection.log' , 
            level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)
    logging.info("Arguments: %s" % args)


    main = Runner(threshold,startTime,endTime,timeWindow,
            countryFilters,asnFilters,proximityFilters,
            args.topicAtlas, args.topicBurst)
    main.run()

