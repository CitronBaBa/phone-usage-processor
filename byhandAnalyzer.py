
from datetime import datetime, timedelta

import pytz
from dateutil import parser

from util import saveAsCsv

outputDirectory = './output/byhandAnalyzer'
treatAsTimezone = 'Asia/Shanghai'

def analyzeByhand(inputObjects):
    results = []
    byAppResults = []

    for inputObj in inputObjects:
        buckets = inputObj['data']['buckets']
        filename = inputObj['filename']

        for bucketName, bucket in buckets.items():
            # all-in-one group
            newResults = analyzeBucket(bucket, lambda x: ())
            newResults.sort(key=lambda x: (x['date'], -x['duration']))
            
            # group by app
            newByAppResults = analyzeBucket(bucket, byApp)
            newByAppResults.sort(key=lambda x: (x['date'], -x['duration']))
            
            # populate data for csv
            results.extend([ [filename, bucketName, x['date'], x['duration'] ] for x in newResults])
            byAppResults.extend([ [filename, bucketName, x['date'], x['app'], x['duration'] ] for x in newByAppResults])

    saveAsCsv(outputDirectory, 'perDay.csv', [ ['fromFile', 'bucketName', 'date', 'duration'] ] + results)
    saveAsCsv(outputDirectory, 'perDayAndApp.csv', [ ['fromFile', 'bucketName', 'date', 'app', 'duration'] ] + byAppResults)


def analyzeBucket(bucket, groupKeyLambda):
    events = bucket['events']
    eventGroups = {}

    # group events
    for event in events:
        groupKey = groupKeyLambda(event)
        if groupKey in eventGroups:
            eventGroups[groupKey].append(event)
        else:
            eventGroups[groupKey] = [event]

    # sum them up
    results = []
    for groupKey, groupEvents in eventGroups.items():
        dayResults = sumEvents(groupEvents)

        # insert group key's content back to result objects 
        for dayResult in dayResults:
            for (name, value) in groupKey:
                dayResult[name] = value

        results.extend(dayResults)

    return results
                


def sumEvents(events):
    if len(events) == 0:
        return []

    for event in events:
        targetTz = pytz.timezone(treatAsTimezone)
        date = parser.parse(event['timestamp']).astimezone(targetTz)
        event['_startTs'] = date
        event['_endTs'] = date + timedelta(seconds=event['duration'])

    # sort by date
    sortedEvents = sorted(events, key=lambda event: event['_startTs'])

    # de-duplication: merge overlapping events 
    mergedEvents = []
    current = sortedEvents[0]
    for event in sortedEvents[1:]:
        if event['_startTs'] <= current['_startTs']:
            current['_endTs'] = max(current['_endTs'],  event['_endTs'])
        else:
            mergedEvents.append(current)
            current = event
    
    mergedEvents.append(current)


    # calculate duration per date 
    byDates = {}
    for event in mergedEvents:
        start = event['_startTs']
        end = event['_endTs']
        startDate = str(start.date())

        if startDate in byDates:
            byDates[startDate] += abs((end - start).total_seconds())
        else:
            byDates[startDate] = abs((end - start).total_seconds())
            
        # TODO: handle cross date event better 
        # end = str(event['_endTs'].date())
        # if start != end:
        #     byDates[start]
        
    results = []
    for date, duration in byDates.items():
        results.append({
            'date': date,
            'duration': round(duration / 3600, 2)
        })

    return results    



def byApp(event):
    app = event['data'].get('app') or '_unknown_'
    return (('app', app),)
