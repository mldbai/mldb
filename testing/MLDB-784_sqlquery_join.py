# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import json

def parse_response(res):
    return json.loads(res['response'])

def check_res(res, value):
    if res['statusCode'] != value:
        mldb.log(parse_response(res))
        assert False
    return parse_response(res)

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'})

ds2 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds2'})

ds1.record_row('row_0', [['x', 0, 0], ['y', 0, 0]])
ds1.record_row('row_1', [['x', 1, 0], ['y', 2, 0]])
ds1.record_row('row_2', [['x', 2, 0], ['y', 4, 0]])
ds1.record_row('row_3', [['x', 3, 0], ['y', 6, 0]])
ds1.record_row('row_4', [['x', 4, 0], ['y', 8, 0]])

ds2.record_row('row_0', [['a', 0, 0]])
ds2.record_row('row_1', [['a', 3, 0]])
ds2.record_row('row_2', [['a', 6, 0]])
ds2.record_row('row_3', [['a', 9, 0]])
ds2.record_row('row_4', [['a', 12, 0]])

ds1.commit()
ds2.commit()

# This function returns the row corresponding to the `id` passed from the first
# dataset
res = mldb.perform('PUT', '/v1/functions/patate', [], {
    'type': 'sql.query',
    'params': {
        'query': 'select * from ds1 where rowName() = $id'
    }
})

check_res(res, 201)

# Make sure the function works properly
res = mldb.perform('GET', '/v1/functions/patate/application',
                   [['input', '{"id":"row_2"}']],{})
response = check_res(res, 200)
assert response["output"]['x'] == 2
assert response["output"]['y'] == 4

# Check the function's info output

info = json.loads(mldb.perform('GET', '/v1/functions/patate/info')["response"])
mldb.log(info)

# now it should do the join
res = mldb.perform('GET', '/v1/query',
                   [
                       ['q', 'SELECT a, patate({rowName() as id}) as * from ds2 where rowName() = \'row_2\''],
                       ['format', 'aos']],
                   {})

response = check_res(res, 200)
assert response[0]['a'] == 6
assert response[0]['y'] == 4
assert response[0]['x'] == 2


# now test the groupBy in sql.query
res = mldb.perform('PUT', '/v1/functions/poil', [], {
    'type': 'sql.query',
    'params': {
        'query' : {
            'select': 'min(x), max(y)',
            'from': {'id': 'ds1'},
            'where': 'x <= $x_max',
            'groupBy': 'true'
        }
    }
})

check_res(res, 201)

res = mldb.perform('GET', '/v1/functions/poil/application',
                   [['input', '{"x_max": 3}']], {})

mldb.log(json.loads(res["response"]))


check_res(res, 200)

# TODO assert on the output

mldb.script.set_return('success')
