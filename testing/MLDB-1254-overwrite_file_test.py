#
# MLDBFB-295-overwrite_file_test.py
# Mich, 2016-01-12
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#

import tempfile
import os
import datetime

if False:
    mldb = None


def log(thing):
    mldb.log(str(thing))


def mldb_perform(*args, **kwargs):
    res = mldb.perform(*args, **kwargs)
    assert res['statusCode'] in [200, 201], str(res)
    return res

ds_file = tempfile.NamedTemporaryFile(prefix=os.getcwd() + '/build/x86_64/tmp')

def create_dataset():
    log("Create scores dataset")
    url = '/v1/datasets/ds_write'
    mldb_perform('PUT', url, [], {
        'type' : 'beh.mutable',
    })

    now = datetime.datetime.now().isoformat() + 'Z'

    for i in xrange(10):
        mldb_perform('POST', url + '/rows', [], {
            'rowName' : 'user{}'.format(i),
            'columns' : [['score', i, now],
                         ['prob', i * 2, now],
                         ['index', i * 3, now]]
        })

    mldb_perform('POST', url + '/commit', [], {})

def bucket_dataset(buckets):
    mldb_perform('POST', '/v1/procedures', [], {
        'type' : 'bucketize',
        'params' : {
            'inputData' : 'SELECT * FROM ds_write ORDER BY score',
            'outputDataset' : {
                'id' : 'bucketed',
                'type' : 'beh.mutable'
            },
            'percentileBuckets' : buckets,
            'runOnCreation' : True
        }
    })


def save_reload_dataset():
    mldb_perform('PUT', '/v1/datasets/merged', [], {
        'type' : 'merged',
        'params' : {
            'datasets' : [
                {'id' : 'ds_write'},
                {'id' : 'bucketed'}
            ]
        }
    })

    mldb_perform('POST', '/v1/procedures', [], {
        'type' : 'transform',
        'params' : {
            'inputData' : 'SELECT * FROM merged',
            'outputDataset' : {
                'type' : 'beh.mutable',
                'params' : {
                    'dataFileUrl' : 'file:///' + ds_file.name
                }
            },
            'runOnCreation' : True
        }
    })

    mldb_perform('PUT', '/v1/datasets/ds_read', [], {
        'type' : 'beh',
        'params' : {
            'dataFileUrl' : 'file:///' + ds_file.name
        }
    })


create_dataset()
bucket_dataset({ '1' : [0, 10], '2' : [10, 30] })
save_reload_dataset()

bucket_dataset({ '1' : [0, 40], '2' : [60, 80] })
save_reload_dataset()

mldb.script.set_return("success")
