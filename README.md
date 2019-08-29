# Disco-Kafka
Internet outage detection with RIPE Atlas disconnections

## Usage
#### Running in Live mode
To start disco in live mode, you just have to specify the burst level threshold (**-t**).
```
$ python3.7 src/run.py -t 7
```
We can change the time window over which the burst detection is computed with the --timeWindow option.

#### Running for a time period
Optionally, you can give start (**-s**) and end times (**-e**)
```
python3.7 src/run.py -t 7 -s 2016-02-23T03:00:00 -e 2016-02-23T23:00:00
```

#### Filtering by Country, ASN or Proximity (50 km radius)
You can give Country (**-c**), ASN (**-a**), Lat,Long (**-p**) lists for filtering
```
python3.7 src/run.py -t 7 -c ["BT","AT","PK"] -a [57169,373] -p [[16.4375,47.3695],[15.4375,47.3695]]
```

To see a list of other optional arguments:
```
$ python3.7 src/run.py -h
```

## Architecture
![architecture](https://github.com/InternetHealthReport/disco-kafka/blob/master/misc/discoKafkaArchitecture.png)
