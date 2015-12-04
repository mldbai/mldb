# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
import datetime
import difflib

dataset_index = 1

def run_transform(when, format):
    global dataset_index
    dataset_index += 1 
    result = mldb.perform("PUT", "/v1/procedures/when_procedure", [], 
                          {
                              "type": "transform",
                              "params": {
                                  "inputDataset": { "id": "dataset1" },
                                "outputDataset": { "id": "dataset_out_" + str(dataset_index), "type":"sparse.mutable" },
                                  "select": "*",
                                  "when": when
                            }
    })

    assert result['statusCode'] == 201, "failed to create the procedure with a WHEN clause: %s" % result['response']

    result = mldb.perform("POST", "/v1/procedures/when_procedure/runs")
    #mldb.log(result)
    assert result['statusCode'] == 201, "failed to run the procedure: %s" % result['response']

    result = mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset_out_" + str(dataset_index)],
                                               ['format', format]])
    assert result['statusCode'] == 200, "failed to get the transformed dataset"
    rows = json.loads(result["response"])
    return rows

def load_test_dataset():
    ds1 = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'dataset1'})

    ds1.record_row('user1', [['x', 1, same_time_tomorrow], ['y', 2, same_time_tomorrow]])
    ds1.record_row('user2', [['x', 3, now], ['y', 4, now]])
    ds1.commit()

def compare_json(json1, json2, format):
    if json1 != json2:
        mldb.log("output format differ:\n")
        for line in difflib.ndiff(json1.splitlines(), json2.splitlines()):
            mldb.log(line)
        assert json1 == json2, "difference in the way null values are outputted in format %s" % format

now = datetime.datetime.now()
later = now + datetime.timedelta(seconds=1)
same_time_tomorrow = now + datetime.timedelta(days=1)

load_test_dataset()

formats = ['full', 'sparse', 'soa', 'aos', 'table']

for format in formats:
    result = mldb.perform('GET', '/v1/query', [
        ['q', "SELECT * FROM dataset1 WHEN timestamp() > '%s'" % later], 
        ['format', format]])
    rows1 = json.dumps( json.loads(result['response']), indent=4, sort_keys=True )

    result = mldb.perform('GET', '/v1/datasets/dataset1/query', [
        ["when", "timestamp() > '%s'" % later],
        ['format', format]], {})
    rows2 = json.dumps( json.loads(result['response']), indent=4, sort_keys=True )
    
    response = run_transform("timestamp() > '%s'" % later, format)
    rows3 = json.dumps( response, indent=4, sort_keys=True, default=str )

    compare_json(rows1, rows2, format)
    compare_json(rows2, rows3, format)

mldb.script.set_return('success')

