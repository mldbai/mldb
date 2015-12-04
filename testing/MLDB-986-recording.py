# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'example'
}

ds = mldb.create_dataset(dataset_config)

out = mldb.perform('POST', '/v1/datasets/example/rows', [], {"rowName": "first row","columns": [["x", {"num" : "NaN"}, 0]]})
mldb.log(out)

out = mldb.perform('POST', '/v1/datasets/example/rows', [], {"rowName": "second row","columns": [["y", {"ts" : "1969-07-20T01:02:03.000Z"}, 0]]})
mldb.log(out)

out = mldb.perform('POST', '/v1/datasets/example/rows', [], {"rowName": "third row","columns": [["z", {"num" : "Inf"}, 0]]})
mldb.log(out)

out = mldb.perform('POST', '/v1/datasets/example/rows', [], {"rowName": "fourth row","columns": [["w", {"interval" : "1D"}, 0]]})
mldb.log(out)

ds.commit()

result = mldb.perform('GET', '/v1/query', [['q', 'select x + 1 as output from example where x IS NOT null']])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]["num"] == "NaN"

result = mldb.perform('GET', '/v1/query', [['q', "select y + INTERVAL '2D' as output from example where y IS NOT null"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]["ts"] == "1969-07-22T01:02:03Z"

result = mldb.perform('GET', '/v1/query', [['q', 'select z + 1 as output from example where z IS NOT null']])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]["num"] == "Inf"

result = mldb.perform('GET', '/v1/query', [['q', 'select w + INTERVAL "1W" as output from example where w IS NOT null']])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]["interval"] == "8D"

#MLDB-955

result = mldb.perform('GET', '/v1/query', [['q', 'select x + 1 as output from example where x IS NOT null'], ['format', 'table']])
mldb.log(result)
assert json.loads(result['response'])[1][1] == "NaN"

result = mldb.perform('GET', '/v1/query', [['q', 'select z + 1 as output from example where z IS NOT null'], ['format', 'table']])
mldb.log(result)
assert json.loads(result['response'])[1][1] == "Inf"

result = mldb.perform('GET', '/v1/query', [['q', 'select y as output from example where y IS NOT null'], ['format', 'table']])
mldb.log(result)
assert json.loads(result['response'])[1][1] == "1969-07-20T01:02:03Z"

result = mldb.perform('GET', '/v1/query', [['q', 'select w as output from example where w IS NOT null'], ['format', 'table']])
mldb.log(result)
assert json.loads(result['response'])[1][1] == "1D"

mldb.script.set_return("success")
