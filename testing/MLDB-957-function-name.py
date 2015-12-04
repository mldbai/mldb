# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'example'
}

dataset = mldb.create_dataset(dataset_config)

dataset.record_row('row1', [ [ "x", 15, 0 ] ])

mldb.log("Committing dataset")
dataset.commit()

result = mldb.perform('GET', '/v1/query', [['q', 'select power(x, 2) from example']])
mldb.log(result)

assert result["statusCode"] == 200
assert json.loads(result['response'])[0]['columns'][0][1] == 225


result = mldb.perform('GET', '/v1/query', [['q', 'select POWER(x, 2) from example']])
mldb.log(result)

assert result["statusCode"] >= 400

mldb.script.set_return('success')
