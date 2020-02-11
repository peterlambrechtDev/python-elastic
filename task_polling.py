#!/usr/local/bin/python3

import properties
import requests
from time import sleep
from decimal import Decimal

# _tasks?actions=*reindex&wait_for_completion=false

def getOngoingTasks():
    return requests.get(properties.destHost + '/_tasks?actions=*reindex&wait_for_completion=false', auth=(properties.destUser, properties.destPass)).json()

def pollTasksTillFinished():
    ongoingReindexTasks = getOngoingTasks()

    if ongoingReindexTasks['nodes'] == {}:
        print('no tasks are running')

    while ongoingReindexTasks['nodes'] != {}:
        ongoingReindexTasks = getOngoingTasks()
        nodes = ongoingReindexTasks['nodes']

        # print(nodes)

        tasksNum = 0

        for node in nodes:
            if nodes[node]:
                tasks = nodes[node]['tasks']
                for task in tasks:
                    tasksNum = tasksNum + 1
                    print(task + ' with running time of ' + str(((int(tasks[task]['running_time_in_nanos']) / 1000000) / 1000) / 60) + ' minutes')

            

        print(str(tasksNum) + ' tasks running')

        sleep(60)

pollTasksTillFinished()