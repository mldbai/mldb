# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#I should be able to create a dataset x, add a row with name françois and then query select * from x without error
#right now I get Error executing non-grouped query: cannot call toString on utf8 string
import json

datasetConfig = {
    'type': 'sparse.mutable',
    'id': 'non-ascii-row'
}

dataset = mldb.create_dataset(datasetConfig)
 
import datetime
ts = datetime.datetime.now()

dataset.record_row('françois', [['x', 1.5,   ts], ['label', '0', ts]])
dataset.commit()

result = mldb.perform('GET', '/v1/query', [['q', 'select * from "non-ascii-row"']])
assert result['statusCode'] == 200, 'failed get'
assert json.loads(result['response'])[0]['rowName'] == unicode('françois',encoding='utf-8'), 'failed non-ascii support'

mldb.script.set_return('success')

