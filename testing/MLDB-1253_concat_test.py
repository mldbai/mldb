#
# MLDB-1253_concat_test.py
# Mich, 2016-01-13
# Copyright (c) 2016 Datacratic Inc. All rights reserved.
#

import json
import traceback

if False:
    mldb = None

_test_cases = []


def log(thing):
    mldb.log(str(thing))


def perform(*args, **kwargs):
    res = mldb.perform(*args, **kwargs)
    assert res['statusCode'] in [200, 201], str(res)
    return res


def query(query):
    res = perform('GET', '/v1/query', [['q', query], ['format', 'table']])
    return json.loads(res['response'])


def record_row(ds_url, row_name, cols):
    perform('POST', ds_url + '/rows', [], {
        'rowName' : row_name,
        'columns' : cols
    })


def test_case(*args, **kwargs):
    _test_cases.append(args[0])


def run_tests():
    success = 0
    errors = 0
    for test_case in _test_cases:
        log('Starting test: ' + test_case.__name__)
        try:
            test_case()
            log(test_case.__name__ + ' SUCCESS')
            success += 1
        except Exception:
            log(test_case.__name__ + ' FAILED')
            errors += 1
            log(traceback.format_exc())

    log("{}/{} test(s) passed".format(success, success + errors))
    if errors:
        raise Exception("{} test(s) failed".format(errors))


def create_sample_dataset():
    url = '/v1/datasets/sample'
    perform('PUT', url, [], {
        'type' : 'sparse.mutable',
    })
    record_row(url, 'row1', [['colA', 'val1A', 0], ['colB', 'val1B', 0]])
    record_row(url, 'row2', [['colA', 'val2A', 0], ['colC', 'val2C', 0]])
    perform('POST', url + '/commit', [], {})


@test_case
def test_default():
    res = query('SELECT concat({*}) FROM sample')
    assert res[1][1] == 'val1A,val1B'
    assert res[2][1] == 'val2A,val2C'


@test_case
def test_keep_nulls():
    res = query('SELECT concat({*}, {skipNulls: false}) FROM sample')
    assert res[1][1] == 'val1A,val1B,'
    assert res[2][1] == 'val2A,,val2C'


@test_case
def test_separator():
    res = query('SELECT concat({*}, {separator: \':\'}) FROM sample')
    assert res[1][1] == 'val1A:val1B'
    assert res[2][1] == 'val2A:val2C'


@test_case
def test_column_value():
    res = query('SELECT concat({*}, {columnValue: false}) FROM sample')
    assert res[1][1] == 'colA,colB'
    assert res[2][1] == 'colA,colC'


@test_case
def test_bad_params():
    try:
        query('SELECT concat({*}, {patate: 1}) FROM sample')
    except AssertionError:
        pass
    else:
        assert False, 'should not be here'


@test_case
def test_alias():
    res = query('SELECT concat({*}) AS alias FROM sample')
    assert res[0][1] == 'alias'


@test_case
def test_partial_columns():
    res = query('SELECT concat({colA, colC}) FROM sample')
    assert res[1][1] == 'val1A'
    assert res[2][1] == 'val2A,val2C'


@test_case
def test_static_columns():
    res = query('SELECT concat({colA, \'static\', colB}) FROM sample')
    assert res[1][1] == 'val1A,static,val1B'
    assert res[2][1] == 'val2A,static'


@test_case
def test_static_columns_name():
    res = query(
        "SELECT concat({colA, 'static', colB}, {columnValue: false}) "
        "FROM sample")
    assert res[1][1] == "colA,'static',colB"
    assert res[2][1] == "colA,'static'"


if __name__ == '__main__':
    create_sample_dataset()
    run_tests()
    mldb.script.set_return("success")
