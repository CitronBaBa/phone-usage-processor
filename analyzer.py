import csv
import os
import sqlite3

# assuming we stored utc time and convert back to target timezone for analysis
timezoneModifier = '+08:00'

shouldSaveCsvFile = True
csvFileDirectory = './output'

def analyzeAll(conn: sqlite3.Connection):
    analyzePerDayAndApp(conn)
    analyzePerDay(conn)

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
        'perDayAndApp.csv', [['fromFile', 'bucketName', 'day', 'app', 'durationSum']]+result)


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
        'perDay.csv', [['fromFile', 'bucketName', 'day', 'durationSum']]+result)


def saveAsCsv(filename, records):
    outputPath = os.path.join(csvFileDirectory, filename)

    # microst excel only recognize utf-16, change to utf-8 if problematic
    with open(outputPath, 'w', newline='', encoding='utf-16') as file:
        writer = csv.writer(file, dialect='excel',)
        writer.writerows(records)
