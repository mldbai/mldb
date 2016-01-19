# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
import datetime
import random

def load_test_dataset():
    ds1 = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'dataset1'})

    row_count = 10
    for i in xrange(row_count - 1):
        # row name is x's value
        ds1.record_row(str(i), [['x', i, same_time_tomorrow], ['y', i, now]])
        
    ds1.record_row(str(row_count - 1), [['x', 9, same_time_tomorrow], ['y', 9, same_time_tomorrow]])
    ds1.commit()

now = datetime.datetime.now()
same_time_tomorrow = now + datetime.timedelta(days=1)
in_two_hours = now + datetime.timedelta(hours=2)

load_test_dataset()

def validate1(result):
    mldb.log(json.loads(result['response']))
    assert result['statusCode'] == 200
    rows = json.loads(result["response"])
    for row in rows:
        if row['rowName'] != 9:
            assert len(row["columns"]) == 1, 'expected x to be filtered out'
        else:
            assert 'columns 'not in row, 'expected x and y to be filtered out'

#validate1(mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1 WHEN timestamp() < when(x)"]]))
#validate1(mldb.perform('GET', '/v1/datasets/dataset1/query',  [['when', "timestamp() < when(x)"]]))


def validate2(result):
    mldb.log(json.loads(result['response']))
    assert result['statusCode'] == 200
    rows = json.loads(result["response"])
    assert len(rows) == 1 and  rows[0]['rowName'] == 9, 'expected where clause to filter all but row 9'
    assert len( rows[0]['columns']) == 2, 'expected the two tuples to be preserved by WHEN clause'

validate2(mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1 WHEN timestamp() = when(x) WHERE x = 9"]]))
#validate2(mldb.perform('GET', '/v1/datasets/dataset1/query', [['when', 'timestamp() = when(x) '], ['where', 'x = 9']]))

def validate3(result):
    mldb.log(json.loads(result['response']))
    assert result['statusCode'] == 200
    rows = json.loads(result["response"])
    for row in rows:
        if row['rowName'] != 9:
            assert len(row["columns"]) == 1, 'expected y to be filtered out'
        else:
            assert len(row["columns"]) == 2, 'expected x and y to be preserved'

#validate3(mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1 WHEN timestamp() > now()"]]))
#validate3(mldb.perform('GET', '/v1/datasets/dataset1/query', [['when', 'timestamp() > now()']]))

def validate4(result):
    mldb.log(json.loads(result['response']))
    assert result['statusCode'] == 200
    rows = json.loads(result["response"])
    for row in rows:
        if row['rowName'] != 9:
            assert len(row["columns"]) == 1, 'expected y to be filtered out'
        else:
            assert len(row["columns"]) == 2, 'expected x and y to be preserved'

#validate4(mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1 WHEN timestamp() BETWEEN now() AND now() + INTERVAL '1W'"]])
#validate4(mldb.perform('GET', '/v1/datasets/dataset1/query', [['when', "timestamp() BETWEEN now() AND now() + INTERVAL '1W'"]]))


def validate5(result):
    mldb.log(json.loads(result['response']))
    assert result['statusCode'] == 200
    rows = json.loads(result["response"])
    for row in rows:
        assert len(row["columns"]) == 2, 'expected x and y to be preserved'

#validate5(mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1 WHEN timestamp() BETWEEN now() - INTERVAL '1d' AND when({*})"]]))
#validate5(mldb.perform('GET', '/v1/datasets/dataset1/query', [['when', "timestamp() BETWEEN now() - INTERVAL '1d' AND when({*})"]]))

def validate6(result):
    mldb.log(json.loads(result['response']))
    assert result['statusCode'] == 200
    rows = json.loads(result["response"])
    for row in rows:
        assert 'columns' not in row, 'expected all values to be filtered out'

#validate6( mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1 WHEN timestamp() BETWEEN when({*}) + INTERVAL '1s' AND '2026-01-01'"]]))
#validate6(mldb.perform('GET', '/v1/datasets/dataset1/query', [['when', "timestamp() BETWEEN when({*}) + INTERVAL '1s' AND '2026-01-01'"]]))

def validate7(result):
    mldb.log(json.loads(result['response']))
    assert result['statusCode'] == 200
    rows = json.loads(result["response"])
    for row in rows:
        if row['rowName'] != 9:
            assert 'columns' not in row, 'expected x and y to be filtered out'
        else:
            assert len(row["columns"]) == 2, 'expected x and y to be preserved'
            
#validate7( mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1 WHEN when(y) > '%s' + INTERVAL '2s'" % now]]))
#validate7(mldb.perform('GET', '/v1/datasets/dataset1/query', [['when', "when(y) > '%s' + INTERVAL '2s'" % now]]))

mldb.script.set_return('success')
