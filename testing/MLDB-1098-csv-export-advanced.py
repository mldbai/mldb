# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#
# MLDB-1098-csv-export-advanced.py
# Mich, 2015-11-18
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

import tempfile

if False:
    mldb = None

tmp_file = tempfile.NamedTemporaryFile(dir='build/x86_64/tmp')


def log_tmp_file():
    f = open(tmp_file.name, 'rt')
    for line in f.readlines():
        mldb.log(line[:-1])


def create_dataset():
    res = mldb.perform('PUT', '/v1/datasets/myDataset', [], {
        'type' : 'sparse.mutable'
    })
    assert res['statusCode'] == 201, str(res)

    def record_row(row_name, columns):
        res = mldb.perform('POST', '/v1/datasets/myDataset/rows', [], {
            'rowName' : row_name,
            'columns' : columns
        })
        assert res['statusCode'] == 200, str(res)

    record_row(1, [['A', 'A1', 0]])
    record_row(2, [['B', 'B2', 0]])
    record_row(3, [['C', 'C3', 0]])
    record_row(4, [['A', 'A4', 0], ['C', 'C4', 0]])

    res = mldb.perform('POST', '/v1/datasets/myDataset/commit', [], {})
    assert res['statusCode'] == 200, str(res)


def assert_file_content(lines_expect):
    f = open(tmp_file.name, 'rt')
    for index, expect in enumerate(lines_expect):
        line = f.readline()[:-1]
        assert line == expect, u"Assert failed at line {}: {} != {}" \
                            .format(index, line, expect)


def select_star_test():
    res = mldb.perform('PUT', '/v1/procedures/export', [], {
        'type' : 'export.csv',
        'params' : {
            'inputDataset' : 'myDataset',
            'dataFileUrl' : 'file://' + tmp_file.name,
            'select' : '*',
            'orderBy' : 'rowName()'
        }
    })
    assert res['statusCode'] == 201, str(res)

    res = mldb.perform('POST', '/v1/procedures/export/runs', [], {})
    assert res['statusCode'] == 201, str(res)


    lines_expect = ['A,B,C',
                    'A1,,',
                    ',B2,',
                    ',,C3',
                    'A4,,C4']
    assert_file_content(lines_expect)


def select_row_name_and_star_test():
    res = mldb.perform('PUT', '/v1/procedures/export', [], {
        'type' : 'export.csv',
        'params' : {
            'inputDataset' : 'myDataset',
            'dataFileUrl' : 'file://' + tmp_file.name,
            'select' : 'rowName() as rowName, *',
            'orderBy' : 'rowName()'
        }
    })
    assert res['statusCode'] == 201, str(res)

    res = mldb.perform('POST', '/v1/procedures/export/runs', [], {})
    assert res['statusCode'] == 201, str(res)

    lines_expect = ['rowName,A,B,C',
                    '1,A1,,',
                    '2,,B2,',
                    '3,,,C3',
                    '4,A4,,C4']
    assert_file_content(lines_expect)


def select_dual_col_test():
    res = mldb.perform('PUT', '/v1/procedures/export', [], {
        'type' : 'export.csv',
        'params' : {
            'inputDataset' : 'myDataset',
            'dataFileUrl' : 'file://' + tmp_file.name,
            'select' : "B, B, B as D",
            'where' : "B = 'B2'"
        }
    })
    assert res['statusCode'] == 201, str(res)

    res = mldb.perform('POST', '/v1/procedures/export/runs', [], {})
    assert res['statusCode'] == 201, str(res)

    lines_expect = ['B,B,D',
                    'B2,B2,B2']
    assert_file_content(lines_expect)


def select_mixed_up_test():
    res = mldb.perform('PUT', '/v1/procedures/export', [], {
        'type' : 'export.csv',
        'params' : {
            'inputDataset' : 'myDataset',
            'dataFileUrl' : 'file://' + tmp_file.name,
            'select' : "'foo' as foo, bar, rowName() as rowName, *, B, B as D",
            'orderBy' : 'rowName()',
        }
    })
    assert res['statusCode'] == 201, str(res)

    res = mldb.perform('POST', '/v1/procedures/export/runs', [], {})
    assert res['statusCode'] == 201, str(res)

    log_tmp_file()

    lines_expect = ['foo,bar,rowName,A,B,C,B,D',
                    'foo,,1,A1,,,,',
                    'foo,,2,,B2,,B2,B2',
                    'foo,,3,,,C3,,',
                    'foo,,4,A4,,C4,,']
    assert_file_content(lines_expect)

create_dataset()
select_star_test()
select_row_name_and_star_test()
select_dual_col_test()
select_mixed_up_test()
mldb.script.set_return("success")
