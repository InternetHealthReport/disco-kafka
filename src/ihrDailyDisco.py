import logging
import sys
import arrow
from run import Runner

# Parameters
threshold = 7
timeWindow = 3600*24

# Kafka topics
topicAtlas = 'ihr_atlas_probe_discolog'
topicBurst = 'ihr_disco_bursts'

# Start/End times
startTime = arrow.utcnow()
# startTime = arrow.get('2020-03-22 00:00')
startTime.replace(microsecond=0, second=0, minute=0, hour=0)
endTime = startTime.shift(days=1) 

# Logging 
FORMAT = '%(asctime)s %(processName)s %(message)s'
logging.basicConfig(
        format=FORMAT, filename='ihr-kafka-disco-detection.log' , 
        level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S'
        )
logging.warning("Started: %s" % sys.argv)

# Start Disco
main = Runner(threshold,
        startTime.timestamp(), endTime.timestamp(), timeWindow,
        [], [], [],
        topicAtlas, topicBurst)
main.run()

