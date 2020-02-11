#!/usr/local/bin/python3

import json
from datetime import datetime
import properties # file that exists in same folder path
import requests
from decimal import Decimal
import task_polling

mb = 1000000
cutoff = 1 * mb
bigCutoff = 9 * mb

smallFileBatch = 750
smallFileQuery = {'range' : {'fileSize': {'lte' : cutoff}}}
mediumFileBatch = 15
mediumFileQuery = {'range' : {'fileSize': {'gt' : cutoff, 'lte' : bigCutoff}}}
largeFileBatch = 4
largeFileQuery = {'range' : {'fileSize': {'gt' : bigCutoff}}}

# report = {'errors':[], 'completed': []}
filename = "migration-" + datetime.now().strftime('%Y-%m-%d_%H:%M:%S') + ".txt"
report = open(filename, "x")
report = open(filename, "a")

remoteAuth = (properties.remoteUser, properties.remotePass)
destAuth = (properties.destUser, properties.destPass)
indices = requests.get(properties.remoteHost + '/_cat/indices/content-*?h=index', auth=remoteAuth)

indexFile = open('indices.txt', 'w')

specificIndex = input("should we do a specific index? Just hit enter for all: ")
if "content-" in specificIndex:
    indicesList = [specificIndex]
else: 
    indicesList = indices.text.split('\n')
print(indicesList, file=indexFile)

def createApplyAliasRequest(aliasList):
    print(aliasList)
    aliasRequest = {'actions':[]}

    for alias in aliasList: 
        add = {'add': {'index': index, 'alias': alias}}
        aliasRequest['actions'].append(add)

    return aliasRequest

def reindex(query, batch, wait):
    reindex = requests.post(properties.destHost + '/_reindex?wait_for_completion=' + wait + '&pretty=true', json={"conflicts": "proceed",
        "source": {
            "remote": {
                "host": properties.remoteHost,
                "username": properties.remoteUser,
                "password": properties.remotePass,
                "socket_timeout": "15m",
                "connect_timeout": "15m"
            },
            "size": batch,
            "query": query,
            "index": index
        },
        "dest": {
            "index": index,
            "version_type": "external"
        }
    }, headers={'Content-Type': 'application/json'}, auth=destAuth)

    if wait == 'false':
        print(reindex.text, file=report)
    return reindex


def determineSizeAndWait(index, docCount):
    wait = 'true'
    indexSize = requests.get(properties.remoteHost + '/_cat/indices/' + index + '?h=store.size', auth=remoteAuth).text
    indexSize = indexSize.strip()
    if (indexSize.endswith('gb')): 
        size = Decimal(indexSize[:-2])
        if size > 1.5 or int(docCount) > 100000:
            print('index larger than 1.5gb and will not wait for reindex response')
            wait = 'false'

    print('size of ' + index +' is: ' + indexSize)
    return wait, indexSize

for index in indicesList:

    if index == '':
        continue

    # GET /_cat/count/<index>
    docCount = requests.get(properties.remoteHost + '/_cat/count/' + index + '?h=count', auth=remoteAuth).text

    destDocCount = requests.get(properties.destHost + '/_cat/count/' + index + '?h=count', auth=destAuth)

    if int(docCount) == 0:
        print('no docs to reindex', file=report)
        print('no docs to reindex')
        continue

    if destDocCount.status_code == 200:
        print(index + ' already exists with ' + destDocCount.text, file=report)
        if int(destDocCount.text) == int(docCount):
            print('matching count of docs, so we will skip', file=report)
            print('matching count of docs, so we will skip ' + index)
            continue

    wait, size = determineSizeAndWait(index, docCount)
    print('reindexing index: ' + index + ' docCount: ' + docCount + ' size: ' + size, file=report)

    aliases = requests.get(properties.remoteHost + '/' + index + '/_aliases', auth=remoteAuth).json()

    aliasList = list(aliases[index]['aliases'].keys())

    if len(aliasList) > 0:
        print('small file batch')
        smallStatus = reindex(smallFileQuery, smallFileBatch, wait)
        if smallStatus.status_code == 200: 
            mediumStatus = reindex(mediumFileQuery, mediumFileBatch, wait)
            print('medium file batch')
            if mediumStatus.status_code == 200:
                print('large file batch')
                largeStatus = reindex(largeFileQuery, largeFileBatch, wait)
                if largeStatus.status_code == 200:
                    print('finished reindex of documents')
                else:
                    print('large files failed: ' + largeStatus.text, file=report)
                    continue
            else:
                print('medium files failed: ' + mediumStatus.text, file=report)
                continue
        else:
            print('small files failed: ' + smallStatus.text, file=report)
            continue

        if wait == 'true':
            destDocCount = requests.get(properties.destHost + '/_cat/count/' + index + '?h=count', auth=destAuth).text
            print('reindex documents completed:' + destDocCount, file=report)
        else:
            print('reindex task started asynchronously', file=report)
    
        aliasResponseCode = requests.post(properties.destHost + '/_aliases', json=createApplyAliasRequest(aliasList), auth=destAuth).status_code

        print('aliases reapplied\n', file=report)
    else: 
        print('Skipping reindex as there were no aliases\n', file=report)

    
print('loop is done', file=report)

report.close()
task_polling.pollTasksTillFinished()

