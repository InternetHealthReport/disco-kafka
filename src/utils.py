"""
Utility Functions
"""

import traceback
import numpy as np
from datetime import datetime
from eventConsumer import EventConsumer
import matplotlib.pyplot as plt

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    try:
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        km = 6367 * c
        return km
    except:
        print(lon1, lat1, lon2, lat2)
        traceback.print_exc()

def plotConnectedProbesGraph(eventData):
    uniqueTimeStamps = []
    eventsByTimeStamps = {}
    for event in eventData:
        eventTimeStamp = event["timestamp"]

        if eventTimeStamp not in uniqueTimeStamps:
            uniqueTimeStamps.append(eventTimeStamp)
            eventsByTimeStamps[eventTimeStamp] = [event]
        else:
            eventsByTimeStamps[eventTimeStamp].append(event)

        eventProbe = event["prb_id"]

    uniqueTimeStamps.sort()

    uniqueTimeStamps = [uniqueTimeStamps[0] - 1] + uniqueTimeStamps

    countValue = 102
    countArray = [countValue]

    for timeStamp in uniqueTimeStamps[1:]:
        events = eventsByTimeStamps[timeStamp]

        con = 0
        discon = 0

        for event in events:
            if event["event"] == "connect":
                con += 1
            else:
                discon += 1

        countValue = countValue + con - discon
        countArray.append(countValue)

    asDates = [datetime.utcfromtimestamp(x).strftime("%d-%b-%Y (%H:%M:%S)") for x in uniqueTimeStamps]

    import pandas as pd
    import matplotlib.dates as mdates

    d = ({"A":asDates,"B":countArray})
    df = pd.DataFrame(data=d)
    df['A'] = pd.to_datetime(df['A'], format="%d-%b-%Y (%H:%M:%S)")

    fig, ax = plt.subplots()
    ax.step(df["A"].values, df["B"].values,'r')
    ax.set_xlim(df["A"].min(), df["A"].max())

    ax.xaxis.set_major_locator(mdates.MinuteLocator((0,30)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%b-%Y (%H:%M:%S)"))

    plt.xticks(rotation=75)
    plt.tight_layout()
    plt.show()

def plotBursts(probeData,start,end,windowR,windowS):
    #Needs minor fixing for eventData
    timeStamp = start
    windowToRead = 3600*24
    windowToSlide = 3600
    timesArray = []
    dataArray = []

    while timeStamp < end:
        eventReader = EventConsumer(timeStamp,windowToRead)
        eventReader.attach(self)
        eventReader.start()

        streamSplitter = StreamSplitter(probeData)
        streams = streamSplitter.getStreams(eventData)

        burstDetector = BurstDetector(streams,probeData,timeRange=windowToRead)
        bursts = burstDetector.detect()

        try:
            bursts = bursts["COUNTRY"]["VE"]

            for datum in bursts:
                score = datum[0]
                tsStart = datum[1]
                tsEnd = datum[2]

                theTimeStart = tsStart
                theTimeEnd = tsEnd

                dataArray.append(score)
                timesArray.append(theTimeStart)

                print(score,theTimeStart,theTimeEnd)
        except:
            pass

        print("No of events: ",len(eventData))
        print("-----")

        eventData = []
        del eventReader, streamSplitter, burstDetector

        timeStamp += windowToSlide

    maxScores = []
    scoreDict = {}
    for score, timeStart in zip(dataArray,timesArray):
        if timeStart not in scoreDict.keys():
            scoreDict[timeStart] = score
        else:
            oldScore = scoreDict[timeStart]

            if score > oldScore:
                scoreDict[timeStart] = score

    import collections
    scoreDict = collections.OrderedDict(sorted(scoreDict.items()))

    print("Scores: ",scoreDict)

    maxScores = scoreDict.values()
    uniqueStartTimes = scoreDict.keys()
    asDates = [datetime.utcfromtimestamp(x).strftime("%d-%b-%Y (%H:%M:%S)") for x in uniqueStartTimes]
    
    import pandas as pd
    import matplotlib.dates as mdates

    d = ({"A":asDates,"B":list(maxScores)})
    df = pd.DataFrame(data=d)
    df['A'] = pd.to_datetime(df['A'], format="%d-%b-%Y (%H:%M:%S)")

    fig, ax = plt.subplots()
    ax.step(df["A"].values, df["B"].values)
    ax.set_xlim(df["A"].min(), df["A"].max())

    ax.xaxis.set_major_locator(mdates.MinuteLocator((0,30)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%b-%Y (%H:%M:%S)"))

    plt.xticks(rotation=75)
    plt.tight_layout()
    plt.show()
    
    print("Done!")