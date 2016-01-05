# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'})

for i in xrange(5):
    ds1.record_row('row_' + str(i),
                   [['x', 'row_0', 0], ['y', i*2, 0]])
ds1.commit()

res = mldb.perform('PUT', '/v1/functions/patate', [], {
    'type': 'sql.query',
    'params': {
        'inputData': 'select * from ds1 where rowName() = $x'
    }
})

assert res['statusCode'] == 201, res['response']

res = mldb.perform('GET', '/v1/functions/patate/info')
mldb.log("patate function info")
mldb.log(json.loads(res["response"]))

# WITH (x)
res = mldb.perform('PUT', '/v1/functions/poil', [], {
        'type': 'sql.expression',
        'params': {
            'expression': 'patate({x})'}})
assert res['statusCode'] == 201

res = mldb.perform('GET', '/v1/functions/poil/info')
mldb.log("poil function info")
mldb.log(json.loads(res["response"]))

res = mldb.perform('GET', '/v1/datasets/ds1/query',
                   [['select', 'poil({*})']],
                   {})

mldb.log("ds1 query")
mldb.log(json.loads(res["response"]))
assert res['statusCode'] == 200

# same but with WITH (*)
res = mldb.perform('PUT', '/v1/functions/poil2', [], {
    'type': 'sql.expression',
    'params': {
        'expression': 'patate({*})'}})
assert res['statusCode'] == 201

res = mldb.perform('GET', '/v1/functions/poil2/info')
# Uncomment this for the MLDB-801 test-case
#assert res['statusCode'] == 200
# also, check that the output looks like this:
# {
#   "input" : {
#       "pins" : {
#          "x" : {
#             "valueInfo" : {
#                "type" : "Datacratic::MLDB::AnyValueInfo"
#             }
#          }
#       }
#    },
#    "output" : {
#       "pins" : {
#          "x" : {
#             "valueInfo" : {
#                "kind" : "scalar",
#                "scalar" : "Datacratic::MLDB::CellValue",
#                "type" : "Datacratic::MLDB::AtomValueInfo"
#             }
#          },
#          "y" : {
#             "valueInfo" : {
#                "kind" : "scalar",
#                "scalar" : "Datacratic::MLDB::CellValue",
#                "type" : "Datacratic::MLDB::AtomValueInfo"
#             }
#          }
#       }
#    }
# }

mldb.log("poil2 function info")
mldb.log(json.loads(res["response"]))

res = mldb.perform('GET', '/v1/datasets/ds1/query',
                   [['select', 'poil2({*})']],
                   {})
mldb.log("query result")
mldb.log(res)
assert res['statusCode'] == 200

mldb.script.set_return('success')
