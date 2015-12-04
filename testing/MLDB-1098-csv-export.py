# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#
# MLDB-1089-csv-export.py
# Mich, 2015-11-16
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

import tempfile
import codecs

if False:
    mldb = None

mldb.log("MLDB can log utf 8 text")
mldb.log("Ǆώύψ")
mldb.log("ăØÆÅ")

res = mldb.perform('PUT', '/v1/datasets/myDataset', [], {
    'type' : 'sparse.mutable'
})
assert res['statusCode'] == 201, str(res)

res = mldb.perform('POST', '/v1/datasets/myDataset/rows', [], {
    'rowName' : 'ascii row',
    'columns' : [
        ['colA', 1, 0],
        ["colB", 2, 0]
    ]
})
assert res['statusCode'] == 200, str(res)

res = mldb.perform('POST', '/v1/datasets/myDataset/rows', [], {
    'rowName' : 'utf8 row',
    'columns' : [
        ['colA', 'Ǆώύψ', 0],
        ["colB", 'ăØÆÅ', 0]
    ]
})
assert res['statusCode'] == 200, str(res)

res = mldb.perform('POST', '/v1/datasets/myDataset/commit', [], {})
assert res['statusCode'] == 200, str(res)

res = mldb.perform('GET', '/v1/query', [['q', 'SELECT * FROM myDataset']])
assert res['statusCode'] == 200, str(res)
mldb.log(res)

tmp_file = tempfile.NamedTemporaryFile(dir='build/x86_64/tmp')

res = mldb.perform('PUT', '/v1/procedures/export', [], {
    'type' : 'export.csv',
    'params' : {
        'inputDataset' : 'myDataset',
        'dataFileUrl' : 'file://' + tmp_file.name,
        'select' : 'rowName() as rowName, colA, colB'
    }
})
assert res['statusCode'] == 201, str(res)

res = mldb.perform('POST', '/v1/procedures/export/runs', [], {})
assert res['statusCode'] == 201, str(res)


def assert_file_content(filename, lines_expect):
    f = codecs.open(filename, 'rt', 'utf8')
    for index, expect in enumerate(lines_expect):
        line = f.readline()[:-1]
        assert line == expect, u"Assert failed at line {}: {} != {}" \
                            .format(index, line, expect)

lines_expect = ['rowName,colA,colB',
                'ascii row,1,2',
                u'utf8 row,Ǆώύψ,ăØÆÅ']
assert_file_content(tmp_file.name, lines_expect)

# import it
res = mldb.perform('PUT', '/v1/datasets/myDataset2', [], {
    'type' : 'text.csv.tabular',
    'params' : {
        'dataFileUrl' : 'file://' + tmp_file.name,
        'named' : 'rowName'
    }
})
assert res['statusCode'] == 201, str(res)

# export it (end of roundtrip)
tmp_file2 = tempfile.NamedTemporaryFile(dir='build/x86_64/tmp')
res = mldb.perform('PUT', '/v1/procedures/export2', [], {
    'type' : 'export.csv',
    'params' : {
        'inputDataset' : 'myDataset2',
        'dataFileUrl' : 'file://' + tmp_file2.name,
        'select' : 'rowName() as rowName, colA, colB'
    }
})
assert res['statusCode'] == 201, str(res)

res = mldb.perform('POST', '/v1/procedures/export2/runs', [], {})
assert res['statusCode'] == 201, str(res)

assert_file_content(tmp_file2.name, lines_expect)

# test quotechar, delimiter, noheader
res = mldb.perform('PUT', '/v1/procedures/export3', [], {
    'type' : 'export.csv',
    'params' : {
        'inputDataset' : 'myDataset2',
        'dataFileUrl' : 'file://' + tmp_file2.name,
        'select' : 'rowName() as rowName, colA, colB',
        'headers' : False,
        'quoteChar' : 'o',
        'delimiter' : ';'
    }
})
assert res['statusCode'] == 201, str(res)

res = mldb.perform('POST', '/v1/procedures/export3/runs', [], {})
assert res['statusCode'] == 201, str(res)

lines_expect = ['oascii roowo;1;2',
                u'outf8 roowo;Ǆώύψ;ăØÆÅ']
assert_file_content(tmp_file2.name, lines_expect)

# test bad target
res = mldb.perform('PUT', '/v1/procedures/export4', [], {
    'type' : 'export.csv',
    'params' : {
        'inputDataset' : 'myDataset2',
        'dataFileUrl' : 'space',
        'select' : 'rowName() as rowName, colA, colB',
        'headers' : False,
        'quoteChar' : 'o',
        'delimiter' : ';'
    }
})
assert res['statusCode'] == 400, str(res)

mldb.script.set_return("success")
