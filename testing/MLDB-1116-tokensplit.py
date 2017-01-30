# -*- coding: utf-8 -*-
#
# MLDB-1116-tokensplit.py
# Mathieu Marquis Bolduc, 2015-11-24
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'example'
})

# create the dataset
ds1.record_row('1', [['x', ":)", 0]])
ds1.record_row('2', [['x', ":P", 0]])
ds1.record_row('3', [['x', "(>_<)", 0]])
ds1.record_row('4', [['x', "(ノಠ益ಠ)ノ彡┻━┻", 0]])
ds1.record_row('5', [['x', "¯\_(ツ)_/¯", 0]])
ds1.record_row('6', [['x', "¯\_(ツ)_/¯¯¯¯¯¯", 0]])

ds1.commit()

result = mldb.put('/v1/functions/tokensplit_function', {
    'type': 'tokensplit',
    'params': {"tokens": "select * from example"}
})

mldb.log(result)

test_str = unicode(
    "whatever :P I do what ¯\_(ツ)_/¯¯¯¯¯¯ I want (>_<) (>_<) watwat :P "
    "(ノಠ益ಠ)ノ彡┻━┻ grrrr :P :P :P",
    encoding='utf-8')
result = mldb.get(
    '/v1/query',
    q="select tokensplit_function({'" + test_str + "' as text}) as query")

mldb.log(result)

response = result.json()

assert response[0]['columns'][0][1] == test_str

test_str = unicode("aaahhhhh ¯\_(ツ)_/¯", encoding='utf-8')
result = mldb.get(
    '/v1/query',
    q="select tokensplit_function({'" + test_str + "' as text}) as query")

mldb.log(result)

response = result.json()

assert response[0]['columns'][0][1] == test_str

query = "'test'"
config = {
    'type': 'tokensplit',
    'params': {
        'tokens': "select ':P', '(>_<)', ':-)'",
        'splitChars': ' ', #split on spaces only
        'splitCharToInsert': ' '
    }
}

result = mldb.put('/v1/functions/split_smiley', config)
mldb.log(result)

test_str = unicode(":P Great day!!! (>_<) (>_<) :P :P :P :-)",
                   encoding='utf-8')

result = mldb.get(
    '/v1/query',
    q="select split_smiley({'" + test_str + "' as text}) as query")

response = result.json()
mldb.log(response)
assert response[0]['columns'][0][1] == test_str, \
    'tokenized string does not match the expected value'

mldb.script.set_return("success")
