#!/usr/local/bin/python3

import properties
import requests
from decimal import Decimal

# _tasks?actions=*reindex&wait_for_completion=false

with open('/Users/plambre/dev/tools/tasksToCancel.txt') as f:
    lines = f.read().splitlines()

for task in lines:
    response = requests.post(properties.destHost + '/_tasks/' + task + '/_cancel', auth=(properties.destUser, properties.destPass))
    print(response.text)

