# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#
# MLDB-1116-tokensplit.py
# Mathieu Marquis Bolduc, 2015-11-24
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

import json

ds1 = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'example'})

#create the dataset
ds1.record_row('1', [['x', ":)", 0]])
ds1.record_row('2', [['x', ":P", 0]])
ds1.record_row('3', [['x', "(>_<)", 0]])
ds1.record_row('4', [['x', "(ノಠ益ಠ)ノ彡┻━┻", 0]])
ds1.record_row('5', [['x', "¯\_(ツ)_/¯", 0]])
ds1.record_row('6', [['x', "¯\_(ツ)_/¯¯¯¯¯¯", 0]])

ds1.commit()

result = mldb.perform('PUT', '/v1/functions/tokensplit_function', [], {
        'type': 'tokensplit',
        'params': {"tokens": "select * from example"}})

mldb.log(result)

result = mldb.perform('GET', '/v1/query', [['q', "select tokensplit_function({'whatever :P I do what ¯\_(ツ)_/¯¯¯¯¯¯ I want (>_<)(>_<) watwat :P(ノಠ益ಠ)ノ彡┻━┻grrrr:P:P:P' as text}) as query"]])

mldb.log(result)

response = json.loads(result["response"])

assert response[0]['columns'][0][1] == unicode("whatever :P I do what ¯\_(ツ)_/¯¯¯¯¯¯ I want (>_<) (>_<) watwat :P (ノಠ益ಠ)ノ彡┻━┻ grrrr :P :P :P", encoding='utf-8')
mldb.script.set_return("success")

result = mldb.perform('GET', '/v1/query', [['q', "select tokensplit_function({'aaahhhhh¯\_(ツ)_/¯' as text}) as query"]])

mldb.log(result)

response = json.loads(result["response"])

assert response[0]['columns'][0][1] == unicode("aaahhhhh ¯\_(ツ)_/¯", encoding='utf-8')

query = "'test'"
config = {
    'type': 'tokensplit',
    'params': {'tokens': "select ':P', '(>_<)', ':-)'",
               'splitchars': ' ', #split on spaces only
               'splitcharToInsert': ' '
           }
}

result = mldb.perform('PUT', '/v1/functions/split_smiley', [], config)

mldb.log(result)
assert result['statusCode'] == 201, 'failed to create split_smiley'

result = mldb.perform('GET', '/v1/query', [['q', "select split_smiley({':PGreat day!!! (>_<)(>_<) :P :P :P:-)' as text}) as query"]])

response = json.loads(result["response"])
mldb.log(response)
assert response[0]['columns'][0][1] == unicode(":P Great day!!! (>_<) (>_<) :P :P :P :-)", encoding='utf-8'), \
'tokenized string does not match the expected value'

mldb.script.set_return("success")
