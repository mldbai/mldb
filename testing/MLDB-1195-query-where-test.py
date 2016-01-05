
#
# MLDB-1195-query-where-test.py
# Mich, 2015-12-15
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

if False:
    mldb = None

def create_ds(name, rowName):
    res = mldb.perform('PUT', '/v1/datasets/' + name, [], {
        'type' : 'sparse.mutable'
    })
    assert res['statusCode'] == 201, str(res)

    res = mldb.perform('POST', '/v1/datasets/' + name + '/rows', [], {
        'rowName' : rowName,
        'columns' : [
            [name, 1, 0],
        ]
    })
    assert res['statusCode'] == 200, str(res)

    res = mldb.perform('POST', '/v1/datasets/' + name + '/commit', [], {})
    assert res['statusCode'] == 200, str(res)

    res = mldb.perform('POST', '/v1/procedures', [], {
        'type' : 'transform',
        'params' : {
            'inputData' : 'select rowName(), "{0}" from {0}'.format(name),
            'outputDataset' : name + 'rows',
            'runOnCreation' : True
        }
    })
    assert res['statusCode'] == 201, str(res)

def run_query(ds_name):
    mldb.log("Running query on " + ds_name)
    query = ('SELECT * FROM {} WHERE colA IS NULL AND colB IS NOT NULL'
             .format(ds_name))
    res = mldb.perform('GET', '/v1/query', [['q', query]])
    assert res['statusCode'] == 200, str(res)
    mldb.log(res)

create_ds('ds1', 'row1')
create_ds('ds2', 'row2')

res = mldb.perform('PUT', '/v1/datasets/merged', [], {
    'type' : 'merged',
    'params' : {
        'datasets' : [
            {'id' : 'ds1'},
            {'id' : 'ds1rows'},
            {'id' : 'ds2'},
            {'id' : 'ds2rows'}
        ]
    }
})
assert res['statusCode'] == 201, str(res)

# The query should work whatever the dataset is
run_query('ds1')
run_query('merged')

mldb.script.set_return("success")
