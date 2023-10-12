#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 10:19:19 2023

@author: chaowang
"""

import json
import os
import warnings

from byhandAnalyzer import analyzeByhand
from dbAnalyzer import analyzeUsingSql

# Directory path where the input JSON files are located
inputFileDirectory = './input'


# 运行
# 注意：目前整个过程是全量的，每次都根据输入重新生成，没有中间数据
def run():
    inputObjects = loadJsonFiles()
    # analyzeUsingSql(inputObjects)
    analyzeByhand(inputObjects)

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


run()
