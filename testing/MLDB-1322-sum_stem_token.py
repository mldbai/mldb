# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#
# MLDB-1322-sum_stem_token.py
# Simon Lemieux, 2016-01-27
# Copyright (c) 2016 Datacratic Inc. All rights reserved.
#

import json

def parse_response(res):
    return json.loads(res['response'])

def check_res(res, code, force_log=False):
    """ Checks that a response has the right code, returns the parsed content
    """
    resp = parse_response(res)
    if res['statusCode'] != code:
        mldb.log('BAD CODE {} != {}'.format(res['statusCode'], code))
        mldb.log(resp)
        assert False
    if force_log:
        mldb.log(resp)
    return resp

def check_output(resp, expected):
    """ Checks that the response of a query matches our expectations """
    parsed_resp = {}
    for row in resp:
        prow = parsed_resp[row['rowName']] = {}
        for col in row['columns']:
            prow[col[0]] = col[1]

    # same rows?
    if sorted(parsed_resp.keys()) != sorted(expected.keys()):
        mldb.log('different row names: {} != {}'.format(
            sorted(parsed_resp.keys()), sorted(expected.keys())))
        assert False
    for row_name, row in parsed_resp.items():
        # same cols per row?
        if sorted(row.keys()) != sorted(expected[row_name].keys()):
            mldb.log('different rows {} != {}'.format(sorted(row.keys()),
                                       sorted(expected[row_name].keys())))
            assert False

        # same values?
        for col_name, val in row.items():
            if val != expected[row_name][col_name]:
                mldb.log('different values in row {} col {} : {} != {}'.format(
                         row_name, col_name, val, expected[row_name][col_name]))
                assert False

def query(q):
    return mldb.perform('GET', '/v1/query', [['q', q]])


ds = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'veggies'})
ds.record_row('row_0', [['txt', 'potato,carrots', 0], ['label',0, 0]])
ds.record_row('row_1', [['txt', 'potato,potatoes,potato', 0], ['label', 0, 0]])
ds.record_row('row_2', [['txt', 'carrot,carrots', 0], ['label',1, 0]])

# We have a dataset that looks like this
#
#      txt                 label
#   potato,carrots           0
# potato,potatoes,potato     0
# carrot,carrots             1
#
# Ultimately I want to count how many of each veggy per label.
# Let's do it step by step.


# Step 1: tokenize
q = '''
SELECT tokenize(txt) as *, label
FROM veggies
'''
res = query(q)
res = check_res(res, 200)

expected = {
    'row_0': {
        'potato': 1,
        'carrots': 1,
        'label': 0},
    'row_1': {
        'potato': 2,
        'potatoes': 1,
        'label': 0},
    'row_2': {
        'carrot': 1,
        'carrots': 1,
        'label': 1}}

# We now have
#
#    potato  potatoes carrot  carrots   label
#       1       nan     nan     1         0
#       2        1      nan    nan        0
#      nan      nan      1      1         1

check_output(res, expected)

# Step 2: tokenize and then stem

conf = {
    "type": "stemmer",
    "params": {
        "language": "english"
    }
}
res = mldb.perform("PUT", "/v1/functions/stem", [], conf)

q = '''
SELECT stem({words: {tokenize(txt) as *}})[words] as *, label
FROM veggies
'''
res = query(q)
res = check_res(res, 200)

expected = {
    'row_0': {
        'potato': 1,
        'carrot': 1,
        'label': 0},
    'row_1': {
        'potato': 3,
        'label': 0},
    'row_2': {
        'carrot': 2,
        'label': 1}}
# We now have
#
#    potato  carrot   label
#       1       1       0
#       3      nan      0
#      nan      2       1

check_output(res, expected)

# Step 3: Let start with just summing the columns
q = '''
SELECT sum(stem({words: {tokenize(txt) as *}})[words]) as *
FROM veggies
'''
res = query(q)
res = check_res(res, 200)

expected = {
    '[]': {
        'potato': 4,
        'carrot': 3}}
check_output(res, expected)

# Step 4: same but grouped by label
q = '''
SELECT sum(stem({words: {tokenize(txt) as *}})[words]) as *, label
FROM veggies
GROUP BY label
'''
res = query(q)
res = check_res(res, 200)

expected = {
    '[0]': {
        'potato': 4,
        'carrot': 1,
        'label': 0},
    '[1]': {
        'carrot': 2,
        'label': 1
    }}
check_output(res, expected)

mldb.script.set_return('success')
