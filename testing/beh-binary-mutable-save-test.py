#
# beh-binary-mutable-save-test.py
# Mich, 2015-12-14
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import tempfile
import os
import json

if False:
    mldb = None

res = mldb.perform('PUT', '/v1/datasets/myDataset', [], {
    'type' : 'beh.binary.mutable'
})
assert res['statusCode'] == 201, str(res)

res = mldb.perform('POST', '/v1/datasets/myDataset/rows', [], {
    'rowName' : 'row1',
    'columns' : [
        ['colA', 1, 0]
    ]
})
assert res['statusCode'] == 200, str(res)

res = mldb.perform('POST', '/v1/datasets/myDataset/rows', [], {
    'rowName' : 'row2',
    'columns' : [
        ['colB', 1, 0],
    ]
})
assert res['statusCode'] == 200, str(res)

tmp_file = tempfile.NamedTemporaryFile(dir='build/x86_64/tmp')

res = mldb.perform('POST', '/v1/procedures', [], {
    'type' : 'transform',
    'params' : {
        'inputData' : 'select colA from myDataset',
        'outputDataset' : {
            'type' : 'beh.binary.mutable',
            'params' : {
                'dataFileUrl' : 'file://' + tmp_file.name,
            }
        },
        'runOnCreation' : True,
        'skipEmptyRows' : True
    }
})
assert res['statusCode'] == 201, str(res)

os.stat(tmp_file.name) # raises if it doesn't exist

res = mldb.perform('PUT', '/v1/datasets/import', [], {
    'type' : 'beh.binary',
    'params' : {
        'dataFileUrl' : 'file://' + tmp_file.name
    }
})
assert res['statusCode'] == 201, str(res)

res = mldb.perform('GET', '/v1/query', [['q', 'SELECT * FROM import']])
assert res['statusCode'] == 200, str(res)
data = json.loads(res['response'])
expect = [
   {
         "columns" : [
                  [ "colA", 1, "1970-01-01T00:00:00Z" ]
               ],
         "rowName" : "row1"
      }
]
assert data == expect, data

mldb.script.set_return("success")
