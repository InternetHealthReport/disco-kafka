import sys
import logging
import arrow
from subprocess import Popen
from kafka.admin import KafkaAdminClient, NewTopic
from run import Runner

if len(sys.argv) < 2:
    sys.exit('usage: %s year')

year = int(sys.argv[1])

topicIn = 'ihr_atlas_probe_discolog_{}'.format(year)
topicOut = 'ihr_disco_bursts_{}'.format(year)
threshold = 7
timeWindow = 3600*24
slideWindow= 3600*24

# Start/End time
startTime = arrow.get('{}-01-01'.format(year))
endTime = startTime.shift(years=1) 

## Create topics with no retention time
admin_client = KafkaAdminClient(
        bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'], 
        client_id='disco_batch_admin')

topic_list = [
        NewTopic(name=topicIn, num_partitions=1, replication_factor=2, 
            topic_configs={'retention.ms':-1}),
        NewTopic(name=topicOut, num_partitions=1, replication_factor=2, 
            topic_configs={'retention.ms':-1}),
        NewTopic(name=topicOut+'_reconnect', num_partitions=1, replication_factor=2, 
            topic_configs={'retention.ms':-1}),
        ]
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
except Exception as e:
    pass
finally:
    admin_client.close()


## Fetch dis/connection events
Popen(
    ['python3', 'eventProducer.py', 
        '-s',str(startTime.shift(seconds=-timeWindow)), 
        '-e', str(endTime.shift(seconds=timeWindow)), 
        '-t', topicIn], 
    stdin=None, stdout=None, stderr=None 
    )

## Run disco
# Logging 
FORMAT = '%(asctime)s %(processName)s %(message)s'
logging.basicConfig(
        format=FORMAT, filename='disco-one-year-batch-{}.log'.format(year) , 
        level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S'
        )
logging.info("Started: %s" % sys.argv)

# Start Disco
main = Runner(threshold,
        startTime.timestamp(), endTime.timestamp(), timeWindow,
        [], [], [],
        topicIn, topicOut, slideWindow)
main.run()


