import os
import json
import collections

# directory where logs live
if os.name == "nt":
    rootDir = "\\\\Lincoln\\Library\\ESPYderivatives\\log\\nginx\\test"
else:
    rootDir = "/media/Library/ESPYderivatives/log/nginx"

#set up uniques output directory
uniques = os.path.join(rootDir, "uniques")

# initalize data
inputData = {}
outputData = {}
outputData["total"] = {}

# Read data from parsed logs
for existingFile in os.listdir(uniques):
    existingDateKey = os.path.splitext(existingFile)[0]
    print ("Reading " + existingDateKey)
    with open(os.path.join(uniques, existingFile)) as json_file:
        existingData = json.load(json_file)
        inputData[existingDateKey] = existingData
        
        
for month in inputData.keys():
    outputData[month] = {}
    for unique in inputData[month]:
        #print (unique)
        #print (inputData[month][unique])
        count, url = inputData[month][unique]
        
        if url in outputData[month].keys():
            outputData[month][url][0] += 1
            outputData[month][url][1] += count
        else:
            outputData[month][url] = [1, count]
            
        if url in outputData["total"].keys():
            outputData["total"][url][0] += 1
            outputData["total"][url][1] += count
        else:
            outputData["total"][url] = [1, count]

sortedData = {} 
for pageValue in outputData.keys():
    print (pageValue)
    sortedData[pageValue] = []
    for unique in outputData[pageValue].keys():
        sortedData[pageValue].append([outputData[pageValue][unique][0], unique, outputData[pageValue][unique][1]])

for unique in reversed(sorted(sortedData["2021-Jun"])):
    uniques, url, totalCount = unique
    if uniques > 10:
        print (url + ": " + str(uniques) + ", " + str(totalCount))