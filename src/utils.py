"""
Utility Functions
"""

import traceback
import numpy as np
from datetime import datetime
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
    ax.step(df["A"].values, df["B"].values)
    ax.set_xlim(df["A"].min(), df["A"].max())

    ax.xaxis.set_major_locator(mdates.MinuteLocator((0,30)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%b-%Y (%H:%M:%S)"))

    plt.xticks(rotation=75)
    plt.show()