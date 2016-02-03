#
# MLDB-1253_concat_test.py
# Mich, 2016-01-13
# Copyright (c) 2016 Datacratic Inc. All rights reserved.
#

import unittest

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class ConcatTest(unittest.TestCase):

    @classmethod
    def record_row(cls, ds_url, row_name, cols):
        mldb.post(ds_url + '/rows', {
            'rowName' : row_name,
            'columns' : cols
        })

    @classmethod
    def setUpClass(cls):
        url = '/v1/datasets/sample'
        mldb.put(url, {
            'type' : 'sparse.mutable',
        })
        cls.record_row(url, 'row1',
                       [['colA', 'val1A', 0], ['colB', 'val1B', 0]])
        cls.record_row(url, 'row2',
                       [['colA', 'val2A', 0], ['colC', 'val2C', 0]])
        mldb.post(url + '/commit')

    def test_default(self):
        res = mldb.query('SELECT concat({*}) FROM sample')
        self.assertEqual(res[1][1], 'val1A,val1B')
        self.assertEqual(res[2][1], 'val2A,val2C')

    def test_separator(self):
        res = mldb.query('SELECT concat({*}, {separator: \':\'}) FROM sample')
        self.assertEqual(res[1][1], 'val1A:val1B')
        self.assertEqual(res[2][1], 'val2A:val2C')

    def test_column_value(self):
        res = mldb.query(
            'SELECT concat({*}, {columnValue: false}) FROM sample')
        self.assertEqual(res[1][1], 'colA,colB')
        self.assertEqual(res[2][1], 'colA,colC')

    def test_bad_params(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.query('SELECT concat({*}, {patate: 1}) FROM sample')

    def test_alias(self):
        res = mldb.query('SELECT concat({*}) AS alias FROM sample')
        self.assertEqual(res[0][1], 'alias')

    def test_partial_columns(self):
        res = mldb.query('SELECT concat({colA, colC}) FROM sample')
        self.assertEqual(res[1][1], 'val1A')
        self.assertEqual(res[2][1], 'val2A,val2C')

    def test_static_columns(self):
        res = mldb.query('SELECT concat({colA, \'static\', colB}) FROM sample')
        self.assertEqual(res[1][1], 'val1A,static,val1B')
        self.assertEqual(res[2][1], 'val2A,static')

    def test_static_columns_name(self):
        res = mldb.query(
            "SELECT concat({colA, 'static', colB}, {columnValue: false}) "
            "FROM sample")
        self.assertEqual(res[1][1], "colA,'static',colB")
        self.assertEqual(res[2][1], "colA,'static'")

if __name__ == '__main__':
    mldb.run_tests()
