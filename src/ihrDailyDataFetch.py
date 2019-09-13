import sys
import logging
import arrow
import eventProducer


topic = 'ihr_atlas_probe_discolog'
# End time
startTime = arrow.utcnow()
startTime.replace(microsecond=0, second=0)
endTime = startTime.shift(days=1) 

# Logging 
FORMAT = '%(asctime)s %(processName)s %(message)s'
logging.basicConfig(
        format=FORMAT, filename='ihr-kafka-disco-data-fetch.log' , 
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
        )
logging.info("Started: %s" % sys.argv)


ep = eventProducer.EventProducer(topic)
# Get one day of live data
ep.startLive(endTime.timestamp)

