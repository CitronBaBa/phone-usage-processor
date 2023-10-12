#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 10:19:19 2023

@author: chaowang
"""

import json
import os
import sqlite3
import warnings

from analyzer import analyzeAll

# Directory path where the input JSON files are located
inputFileDirectory = './input'


# 运行
# 注意：目前整个过程是全量的，每次都根据输入重新生成，没有中间数据
def run():
    inputObjects = loadJsonFiles()
    eventRecords = convert(inputObjects)

    # 将数据注入一个全新的sqlite数据库，便于查询。有需要sqlite可以保存成文件。
    conn = sqlite3.connect(':memory:')

    createAndPopulateTable(conn, eventRecords)
    analyzeAll(conn)

    # 保存到result.db文件，会覆盖之前的
    resultConn = sqlite3.connect('result.db')
    conn.backup(resultConn)
    
    conn.close()
    resultConn.close()

# convert raw input => records
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


def loadJsonFiles():
    # List to store the loaded JSON data
    loadedInputObjects = []

    # Iterate over each file in the directory
    for filename in os.listdir(inputFileDirectory):
        if filename.endswith('.json'):
            file_path = os.path.join(inputFileDirectory, filename)
            # Open the file and load its JSON content
            with open(file_path) as file:
                data = json.load(file)
                loadedInputObjects.append({"data": data, "filename": filename})
        else:
            warnings.warn('unrecognized file:', filename)

    return loadedInputObjects


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

run()
