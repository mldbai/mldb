# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#   MLDB-1025-dataset-output-with-default.py
#   Guy Dumais, 17 November 2015
#   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

import json
from datetime import datetime

dataset_index = 1

   
def create_dataset_and_query(type, date, quantum = None):
    if quantum:
        ds1 = mldb.create_dataset({
            'type': type,
            'id': 'beh',
            'params' : {
                'timeQuantumSeconds' : quantum
            }
        })
    else:
        ds1 = mldb.create_dataset({
            'type': type,
            'id': 'beh'
        })

    sometime = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
    ds1.record_row('row', [['column', 1, sometime]])
    ds1.commit()

    response = mldb.perform('GET', '/v1/query', [['q', 'select * from beh']])['response']
    mldb.perform('DELETE', '/v1/datasets/beh', [])
    return json.loads(response)[0]['columns'][0][2]

def check_equality(received, expected, message):
    msg = 'received: ' + received + ' expected: ' + expected + " " + message
    assert received == expected, msg
        
test_samples = [ 
    ('2015-11-17T13:20:20.956Z', 0.001, '2015-11-17T13:20:20.9552119Z', 'quantization should be in thousandth seconds'),
    ('2015-11-17T13:20:20.956Z', 0.01,  '2015-11-17T13:20:20.959898Z', 'quantization should be in hundredth seconds'),
    ('2015-11-17T13:20:20.956Z', 1,  '2015-11-17T13:20:21Z', 'quantization should be in seconds'),
    ('2015-11-17T13:20:20.956Z', None, '2015-11-17T13:20:21Z', 'default quantization should be in seconds'),
    ('2015-11-17T13:20:20.956Z', 60, '2015-11-17T13:20:00Z', 'quantization should be in minutes'),
    ('2015-11-17T13:20:59.956Z', 60, '2015-11-17T13:20:00Z', 'quantization should be in minutes') # flooring is happening - see MLDBFB-255
]

mutable_datasets = ['sparse.mutable', 'beh.binary.mutable', 'sparse.mutable']

for mutable_dataset in mutable_datasets:
    for timestamp, quantum, expected, msg  in test_samples:
        check_equality(create_dataset_and_query(mutable_dataset, timestamp, quantum), expected, msg)

mldb.script.set_return("success")
