"""
Given a list of disconnected probes, reports when 50% of them are reconnected
"""
from eventConsumer import EventConsumer
from kafka import KafkaProducer
import msgpack

class ProbeTracker():
    def __init__(self,streamType,streamName,startTime,disconnectedProbes):
        self.streamType = streamType
        self.streamName = streamName
        self.startTime = startTime
        self.disconnectedProbes = disconnectedProbes
        self.reconnectedProbes = {}

        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', acks=0,
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            batch_size=65536,linger_ms=4000,compression_type='gzip')

        self.topicName = "ihr_disco_reconnect"

        print("Tracker Started!~")

    def getTimeForLastReconnect(self):
        return max(self.reconnectedProbes.values())

    def pushEventToKafka(self,endTime):
        event = {}
        event["streamtype"] = self.streamType
        event["streamname"] = self.streamName
        event["starttime"] = self.startTime
        event["endtime"] = endTime
        event["reconnectedprobes"] = self.reconnectedProbes

        self.producer.send(self.topicName,event,timestamp_ms=int(endTime*1000))
        self.producer.flush()

    def eventDataProcessor(self,data):
        #get event probe
        eventProbeId = data["prb_id"]
        eventType = data["event"]
        eventTime = data["timestamp"]

        if eventType == "connect":
            if eventProbeId in self.disconnectedProbes.keys():
                disconnectTime = self.disconnectedProbes[eventProbeId]

                if eventTime > disconnectTime:
                    self.reconnectedProbes[eventProbeId] = eventTime


    def start(self):    #returns when done hence notifying calling thread to quit
        timeStamp = self.startTime
        timeWindow = 3600

        while True:
            eventReader = EventConsumer(timeStamp,timeWindow)
            eventReader.attach(self)
            eventReader.start()

            if len(self.reconnectedProbes) > (len(self.disconnectedProbes)/2):
                self.pushEventToKafka(self.getTimeForLastReconnect())
                print("~Reconnect Event!")
                return

            timeStamp += timeWindow

        self.producer.close()








