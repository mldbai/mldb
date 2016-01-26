#
# MLDBFB-308-where-outer-join-test.py
# Mich, 2016-01-13
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

import json

if False:
    mldb = None

def log(thing):
    mldb.log(str(thing))


def perform(*args, **kwargs):
    res = mldb.perform(*args, **kwargs)
    assert res['statusCode'] in [200, 201], str(res)
    return res


def query(query):
    res = perform('GET', '/v1/query', [['q', query], ['format', 'table']])
    return json.loads(res['response'])


def create_dataset():
    log("Create scores dataset")
    url = '/v1/datasets/ds'
    perform('PUT', url, [], {
        'type' : 'sparse.mutable',
    })

    perform('POST', url + '/rows', [], {
        'rowName' : 'userValid',
        'columns' : [['behA', 1, 3]]
    })

    perform('POST', url + '/commit', [], {})


def the_test():
    query("SELECT 1 FROM ds OUTER JOIN (SELECT 2 FROM ds) WHERE behA")

if __name__ == '__main__':
    create_dataset()
    the_test()
    mldb.script.set_return("success")
