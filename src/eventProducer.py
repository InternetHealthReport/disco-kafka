from kafka import KafkaProducer
from datetime import datetime
import time
import json
from ripe.atlas.cousteau import AtlasResultsRequest

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks=0,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=65536,linger_ms=4000,compression_type='gzip')

topicName = "atlasLiveData"

WINDOW = 600
currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
while True:
    try:
        kwargs = {
            "msm_id": 7000,
            "start": datetime.utcfromtimestamp(currentTS-WINDOW),
            "stop": datetime.utcfromtimestamp(currentTS),
        }
        is_success, results = AtlasResultsRequest(**kwargs).create()
        READ_OK = False
        if is_success:
            for ent in results:
                timestamp = ent["timestamp"]
                timestamp = timestamp*1000      #convert to milliseconds
                producer.send(topicName,ent,timestamp_ms=timestamp)
                print("Record pushed")
        READ_OK = True
        time.sleep(WINDOW)
        currentTS += (WINDOW + 1)
    except Exception as e:
        print("Eeech: ",e)
