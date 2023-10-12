import csv
import os
import sqlite3

from util import saveAsCsv

# assuming we stored utc time and convert back to target timezone for analysis
timezoneModifier = '+08:00'
outputDirectory = './output/dbAnalyzer'

def analyzeUsingSql(inputObjects):
    eventRecords = convert(inputObjects)

    # 将数据注入一个全新的sqlite数据库，便于查询。有需要sqlite可以保存成文件。
    conn = sqlite3.connect(':memory:')

    createAndPopulateTable(conn, eventRecords)

    analyzePerDayAndApp(conn)
    analyzePerDay(conn)

    # 保存到result.db文件，会覆盖之前的
    # resultConn = sqlite3.connect('result.db')
    # conn.backup(resultConn)
    # resultConn.close()

    conn.close()


# convert raw input => db records
def convert(inputObjects):
    records = []

    for inputObj in inputObjects:
        filename = inputObj['filename']
        for bucketName, bucket in inputObj['data']['buckets'].items():
            bucketRecords = convertBucket(filename, bucketName, bucket)
            records.extend(bucketRecords)

    return records


def convertBucket(filename, bucketName, bucket):
    records = []

    for event in bucket['events']:
        records.append({
            'fromFile': filename,
            'bucketName': bucketName,
            'bucketId': bucket['id'],
            'bucketClient': bucket['client'],
            'bucketType': bucket['type'],
            'bucketHostname': bucket['hostname'],

            'timestamp': event['timestamp'],
            'duration': event['duration'],
            'app': event['data'].get('app') or '_unknown_',
            'appTitle': event['data'].get('title') or '_unknown_',
            'appStatus': event['data'].get('status') or '_unknown_',
            'appClassname': event['data'].get('classname') or '_unknown_',
            'appPackage': event['data'].get('package') or '_unknown_',
        })

    return records


# create a table and insert the data to it
def createAndPopulateTable(conn: sqlite3.Connection, records):

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS phoneUsageRecords  (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fromFile TEXT,
            bucketName TEXT,
            bucketId TEXT,
            bucketClient TEXT,
            bucketType TEXT,
            bucketHostname TEXT,
            timestamp TEXT,
            duration REAL,
            app TEXT,
            appTitle TEXT,
            appStatus TEXT,
            appClassname TEXT,
            appPackage TEXT
    )''')

    # Insert into the table
    cursor.executemany('''
    INSERT INTO phoneUsageRecords (
        fromFile, bucketName, bucketId, bucketClient, bucketType, bucketHostname,
        timestamp, duration, app, appTitle, appStatus, appClassname, appPackage
    )
    VALUES (
        :fromFile, :bucketName, :bucketId, :bucketClient, :bucketType, :bucketHostname,
        :timestamp, :duration, :app, :appTitle, :appStatus, :appClassname, :appPackage
    )
    ''', records)

    conn.commit()
    cursor.close()


def analyzePerDayAndApp(conn: sqlite3.Connection):
    cursor = conn.cursor()

    cursor.execute(f''' 
    SELECT
        fromFile,
        bucketName,
        date(timestamp, '{timezoneModifier}') AS day,
        app,
        ROUND(SUM(duration)/ 3600, 4) AS durationSum
    FROM
        phoneUsageRecords
    GROUP BY
        fromFile,
        bucketName,
        day,
        app
    ORDER by
        fromFile,
        bucketName,       
        day,
        durationSum desc;
                   ''')
    result = cursor.fetchall()

    saveAsCsv(
        outputDirectory, 'perDayAndApp.csv', [['fromFile', 'bucketName', 'day', 'app', 'durationSum']]+result)


def analyzePerDay(conn: sqlite3.Connection):
    cursor = conn.cursor()

    cursor.execute(f''' 
    SELECT
        fromFile,
        bucketName,
        date(timestamp, '{timezoneModifier}') AS day,
        ROUND(SUM(duration)/ 3600, 4) AS durationSum
    FROM
        phoneUsageRecords
    GROUP BY
        fromFile,
        bucketName,
        day
    ORDER by
        fromFile,
        bucketName,       
        day,
        durationSum desc;
                   ''')
    result = cursor.fetchall()

    saveAsCsv(
        outputDirectory, 'perDay.csv', [['fromFile', 'bucketName', 'day', 'durationSum']]+result)

