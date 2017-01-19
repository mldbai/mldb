#
# MLDB-927-null-row-output.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import json
import datetime
import difflib

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset_index = 1

def run_transform(when, format):
    global dataset_index
    dataset_index += 1
    result = mldb.put("/v1/procedures/when_procedure", {
        "type": "transform",
        "params": {
            "inputData": "select * from dataset1 when " + when,
            "outputDataset": {
                "id": "dataset_out_" + str(dataset_index),
                "type":"sparse.mutable"
            }
        }
    })

    mldb.post("/v1/procedures/when_procedure/runs")

    result = mldb.get('/v1/query',
                      q="SELECT * FROM dataset_out_" + str(dataset_index) + " ORDER BY rowHash()",
                      format=format)
    rows = result.json()
    return rows

def load_test_dataset():
    ds1 = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'dataset1'})

    ds1.record_row('user1', [['x', 1, same_time_tomorrow],
                             ['y', 2, same_time_tomorrow]])
    ds1.record_row('user2', [['x', 3, now], ['y', 4, now]])
    ds1.commit()

def compare_json(json1, json2, format):
    if json1 != json2:
        mldb.log("output format differ:\n")
        for line in difflib.ndiff(json1.splitlines(), json2.splitlines()):
            mldb.log(line)
        assert json1 == json2, \
            "difference in the way null values are outputted in format %s" \
            % format

now = datetime.datetime.now()
later = now + datetime.timedelta(seconds=1)
same_time_tomorrow = now + datetime.timedelta(days=1)

load_test_dataset()

formats = ['full', 'sparse', 'soa', 'aos', 'table']

for format in formats:
    result = mldb.get('/v1/query',
        q="SELECT * FROM dataset1 WHEN value_timestamp() > '%s' ORDER BY rowHash()" % later ,
        format=format)

    rows1 = json.dumps(result.json(), indent=4, sort_keys=True)

    result = mldb.get('/v1/query',
        q = "SELECT * from dataset1 WHEN value_timestamp() > '%s' ORDER BY rowHash()" % later, format=format)

    rows2 = json.dumps(result.json(), indent=4, sort_keys=True)

    response = run_transform("value_timestamp() > '%s'" % later, format)

    rows3 = json.dumps(response, indent=4, sort_keys=True, default=str)

    compare_json(rows1, rows2, format)
    compare_json(rows2, rows3, format)

mldb.script.set_return('success')
